mod read;
mod write;

pub use read::*;
pub use write::*;

use std::{
    borrow::Cow,
    cmp,
    io::{self, Write},
    ops::Deref,
    path::{Path, PathBuf},
    todo,
};

use crate::{
    lsm_kv::{serialization, BloomFilter, CompressionType},
    Error, Lsn, Result,
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize};

fn sst_path(dir: &Path, id: u32) -> PathBuf {
    PathBuf::from(dir).join(format!("sst-{}", id))
}

// Single record.
pub struct SstRecord {
    internal_key: InternalKey<'static>,
    value: Vec<u8>,
}

#[derive(PartialEq, Eq, Clone)]
pub struct InternalKey<'a>(Cow<'a, [u8]>);

impl PartialOrd for InternalKey<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for InternalKey<'_> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        // Order:
        // 1. Key in ascending order.
        // 2. LSN in descending order.
        match self.key().cmp(other.key()) {
            cmp::Ordering::Equal => other.lsn().cmp(&self.lsn()),
            x => x,
        }
    }
}

impl<'a> InternalKey<'a> {
    fn from_buf(buf: &'a [u8]) -> InternalKey<'a> {
        Self(Cow::Borrowed(buf))
    }

    fn from_buf_owned(buf: Vec<u8>) -> InternalKey<'static> {
        InternalKey(Cow::Owned(buf))
    }

    fn new(key: &[u8], lsn: Lsn) -> InternalKey<'_> {
        let mut buf = Vec::with_capacity(key.len() + 8);
        buf.extend_from_slice(key);
        buf.write_u64::<LittleEndian>(lsn).unwrap();
        Self::from_buf_owned(buf)
    }

    fn apply_delta(&mut self, shared: u32, delta: &[u8]) {
        let buf = self.0.to_mut();
        assert!(buf.len() as u32 >= shared);
        buf.resize(shared as usize, 0);
        buf.extend_from_slice(delta);
    }

    fn buf(&self) -> &[u8] {
        &self.0
    }

    pub fn key(&self) -> &[u8] {
        &self.0[..self.0.len() - 8]
    }

    pub fn lsn(&self) -> Lsn {
        let mut slice = &self.0[self.0.len() - 8..];
        slice.read_u64::<LittleEndian>().unwrap()
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
struct ShortInternalKey(Vec<u8>);

impl ShortInternalKey {
    fn new(ik: &InternalKey, prev: Option<&InternalKey>) -> Self {
        // TODO
        Self(ik.buf().to_owned())
    }

    fn buf(&self) -> &[u8] {
        self.0.deref()
    }
}

#[derive(Serialize, Deserialize, Copy, Clone)]
struct BlockHandle {
    offset: u64,
    size: u64,
}

// +---------------+-------------+
// | Footer Layout               |
// +---------------+-------------+
// | index_block   | BlockHandle |
// | filter_block  | BlockHandle |
// | stat_block    | BlockHandle |
// | padding       | [u8]        |
// | magic         | u64 (fixed) |
// +---------------+-------------+
// NOTE: padding is used to sizeof(footer) == FOOTER_SIZE.

const FOOTER_MAGIC: u64 = 0xf12345678abcdef;
const FOOTER_SIZE: u64 = 128;

struct Footer {
    index_handle: BlockHandle,
    filter_handle: BlockHandle,
    stat_handle: BlockHandle,
}

impl serialization::FixedSizeSerializable for Footer {
    fn serialized_size() -> usize {
        FOOTER_SIZE as usize
    }

    fn serialize_into(&self, w: impl Write) -> Result<()> {
        let mut footer_buf = [0u8; FOOTER_SIZE as usize];

        let mut slice = &mut footer_buf[..];
        serialization::serialize_into(&mut slice, &self.index_handle)?;
        serialization::serialize_into(&mut slice, &self.filter_handle)?;
        serialization::serialize_into(&mut slice, &self.stat_handle)?;

        // Write magic.
        let mut slice = &mut footer_buf[(FOOTER_SIZE - 8) as usize..];
        slice.write_u64::<LittleEndian>(FOOTER_MAGIC)?;

        Ok(())
    }

    fn deserialize_from(r: impl io::Read) -> Result<Self> {
        todo!()
    }
}

#[derive(Serialize, Deserialize, Default)]
struct IndexBlock {
    data_handles: Vec<BlockHandle>,
    start_keys: Vec<ShortInternalKey>,
}

#[derive(Serialize, Deserialize, Default)]
struct FilterBlock {
    /// Bloom filters per data block.
    blooms: Vec<BloomFilter>,
}

#[derive(Serialize, Deserialize)]
struct StatBlock {
    // TODO: stat
}

struct DataBlockFooter {
    compression: CompressionType,

    /// Offset of restart array, relative to the beginning of data block.
    restart_array_offset: u32,
    // TODO: checksum?
}

impl serialization::FixedSizeSerializable for DataBlockFooter {
    fn serialized_size() -> usize {
        5
    }

    fn serialize_into(&self, mut w: impl Write) -> Result<()> {
        w.write_u8(self.compression as u8)?;
        w.write_u32::<LittleEndian>(self.restart_array_offset)?;
        Ok(())
    }

    fn deserialize_from(mut r: impl io::Read) -> Result<Self> {
        let comp_raw = r.read_u8()?;
        let compression = match comp_raw {
            x if x == CompressionType::NoCompression as u8 => CompressionType::NoCompression,
            _ => return Err(Error::Corrupted("invalid compression type".into())),
        };

        let restart_array_offset = r.read_u32::<LittleEndian>()?;

        Ok(Self {
            compression,
            restart_array_offset,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lsm_kv::Options;
    use std::sync::Arc;

    use tempdir::TempDir;

    fn temp_dir() -> Result<TempDir> {
        let dir = TempDir::new("sst")?;
        Ok(dir)
    }

    #[test]
    fn internal_key_simple() {
        let bytes = "Hello".as_bytes();
        let num = 0xff12345678;

        let ik = InternalKey::new(bytes, num);
        let buf = ik.buf();

        let ik2 = InternalKey::from_buf(buf);
        assert_eq!(ik2.lsn(), num);
        assert_eq!(ik2.key(), bytes);
    }

    #[test]
    fn seperator_key_simple() {
        let ik1 = InternalKey::new("test123456".as_bytes(), 100);
        let ik2 = InternalKey::new("test1237".as_bytes(), 100);
        let sk = ShortInternalKey::new(&ik1, Some(&ik2));
    }

    fn options(dir: &Path) -> Arc<Options> {
        Arc::new(Options {
            dir: dir.into(),
            bloom_bits_per_key: 20,
            compression: CompressionType::NoCompression,
            data_block_cache: None,
            data_block_restart_interval: 16,
            data_block_size: 4096,
        })
    }

    #[test]
    #[should_panic(expected = "record out of order")]
    fn writer_out_of_order() {
        let temp_dir = temp_dir().unwrap();
        let opt = options(temp_dir.path());
        let mut w = SstWriter::new(opt, 1).unwrap();
        w.push(&SstRecord {
            internal_key: InternalKey::new("hello2".as_bytes(), 3),
            value: vec![0],
        })
        .unwrap();
        w.push(&SstRecord {
            internal_key: InternalKey::new("hello1".as_bytes(), 4),
            value: vec![1],
        })
        .unwrap();
    }
}
