mod read;
mod write;

pub use read::*;
pub use write::*;

use std::{
    borrow::Cow,
    io::{self, Write},
    ops::{Deref},
    path::{Path, PathBuf},
    todo,
};

use crate::{
    lsm_kv::{
        serialization, BloomFilter,
        CompressionType,
    },
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

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct InternalKey<'a>(Cow<'a, [u8]>);

impl<'a> InternalKey<'a> {
    fn from_buf(buf: &'a [u8]) -> InternalKey<'a> {
        Self(Cow::Borrowed(buf))
    }

    fn from_owned_buf(buf: Vec<u8>) -> InternalKey<'static> {
        InternalKey(Cow::Owned(buf))
    }

    fn new(key: &[u8], lsn: Lsn) -> InternalKey<'_> {
        let mut buf = Vec::with_capacity(key.len() + 8);
        buf.extend_from_slice(key);
        buf.write_u64::<LittleEndian>(lsn).unwrap();
        Self::from_owned_buf(buf)
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
