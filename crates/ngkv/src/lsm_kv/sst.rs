use std::{
    borrow::Cow,
    convert::TryFrom,
    fs,
    io::{BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write},
    marker::PhantomData,
    mem,
    ops::{Bound, Deref, RangeBounds},
    path::{Path, PathBuf},
    sync::Arc,
    todo,
};

use crate::{
    file_log_options, key_comparator,
    lsm_kv::{
        serialization, BloomFilter, BloomFilterBuilder, ByteCountedRead, ByteCountedWrite,
        CompressionType, KvOp, Options, ValueOp,
    },
    Error, Lsn, Result,
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use chrono::offset;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serialization::{deserialize_from, FixedSizeSerializable};
use stdx::crc32_io::Crc32Write;

use super::DataBlockCache;

fn sst_path(dir: &Path, id: u32) -> PathBuf {
    PathBuf::from(dir).join(format!("sst-{}", id))
}

// Single record.
#[derive(Clone)]
pub(crate) struct SstRecord<'a> {
    key: Cow<'a, [u8]>,
    lsn: Lsn,
    value: ValueOp<'a>,
}

impl SstRecord<'_> {
    fn internal_key_owned(&self) -> InternalKey<'static> {
        let mut buf = Vec::with_capacity(self.key.len() + 8);
        buf.extend_from_slice(self.key.deref());
        buf.write_u64::<LittleEndian>(self.lsn).unwrap();
        InternalKey(Cow::Owned(buf))
    }

    fn internal_key<'a>(&self, buf: &'a mut Vec<u8>) -> InternalKey<'a> {
        buf.clear();
        buf.extend_from_slice(self.key.deref());
        buf.write_u64::<LittleEndian>(self.lsn).unwrap();
        InternalKey(Cow::Borrowed(buf))
    }
}

struct InternalKey<'a>(Cow<'a, [u8]>);

impl<'a> InternalKey<'a> {
    fn key(&self) -> &[u8] {
        let buf = self.0.deref();
        &buf[..buf.len() - 8]
    }

    fn lsn(&self) -> Lsn {
        let buf = self.0.deref();
        let mut slice = &buf[buf.len() - 8..];
        slice.read_u64::<LittleEndian>().unwrap()
    }

    fn owned(self) -> InternalKey<'static> {
        InternalKey(Cow::Owned(self.0.into_owned()))
    }
}

#[derive(Serialize, Deserialize)]
struct ShortInternalKey(Vec<u8>);

impl ShortInternalKey {
    fn from_internal(ik: &InternalKey<'_>) -> Self {
        Self(ik.0.deref().to_owned())
    }

    fn from_internal_shorten(ik: &InternalKey<'_>, prev: &InternalKey) -> Self {
        // TODO
        Self(ik.0.deref().to_owned())
    }
}

#[derive(Serialize, Deserialize)]
struct BlockHandle {
    offset: u32,
    size: u32,
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
const FOOTER_SIZE: u32 = 64;

const DATA_BLOCK_SIZE: u32 = 2048;
const BLOCK_HANDLE_MAX_SERIALIZED_SIZE: u32 = 10;

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

    fn deserialize_from(r: impl std::io::Read) -> Result<Self> {
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
    // TODO: checksum?
}

impl serialization::FixedSizeSerializable for DataBlockFooter {
    fn serialized_size() -> usize {
        1
    }

    fn serialize_into(&self, mut w: impl Write) -> Result<()> {
        w.write_u8(self.compression as u8)?;
        Ok(())
    }

    fn deserialize_from(mut r: impl std::io::Read) -> Result<Self> {
        let comp_raw = r.read_u8()?;
        let compression = match comp_raw {
            x if x == CompressionType::NoCompression as u8 => CompressionType::NoCompression,
            _ => return Err(Error::Corrupted("invalid compression type".into())),
        };

        Ok(Self { compression })
    }
}

pub(crate) struct SstRangeIter<'a> {
    sst: &'a Sst,
    start: Bound<&'a [u8]>,
    end: Bound<&'a [u8]>,
}

impl Iterator for SstRangeIter<'_> {
    type Item = Result<SstRecord<'static>>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl DoubleEndedIterator for SstRangeIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

pub(crate) struct Sst {
    opt: Arc<Options>,
    data_cache: Option<Arc<DataBlockCache>>,
    index_block: IndexBlock,
    filter_block: FilterBlock,
    stat_block: StatBlock,
    fd: fs::File,
}

fn read_deser_block<T: DeserializeOwned>(
    mut r: impl Read + Seek,
    handle: BlockHandle,
) -> Result<T> {
    r.seek(SeekFrom::Start(handle.offset as u64))?;
    let mut byte_counted = ByteCountedRead::new(&mut r);

    let t = deserialize_from(&mut byte_counted)?;
    if byte_counted.bytes_read() == handle.size as usize {
        Ok(t)
    } else {
        Err(Error::ReportableBug(
            "deserializer read unexpected number of bytes".into(),
        ))
    }
}

impl Sst {
    pub fn open(opt: Arc<Options>, id: u32) -> Result<Sst> {
        let path = sst_path(&opt.dir, id);
        let mut fd = fs::OpenOptions::new().read(true).open(path)?;
        let mut reader = BufReader::new(&mut fd);

        reader.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
        let footer = Footer::deserialize_from(&mut reader)?;

        let index_block = read_deser_block(&mut reader, footer.index_handle)?;
        let filter_block = read_deser_block(&mut reader, footer.filter_handle)?;
        let stat_block = read_deser_block(&mut reader, footer.stat_handle)?;

        Ok(Sst {
            data_cache: opt.data_cache.clone(),
            opt,
            index_block,
            filter_block,
            stat_block,
            fd,
        })
    }

    pub fn range<'s: 'k, 'k>(
        &'s self,
        range: impl RangeBounds<&'k [u8]>,
    ) -> Result<SstRangeIter<'k>> {
        let map_bound = |b| match b {
            Bound::Included(&t) => Bound::Included(t),
            Bound::Excluded(&t) => Bound::Excluded(t),
            Bound::Unbounded => Bound::Unbounded,
        };

        Ok(SstRangeIter {
            sst: self,
            start: map_bound(range.start_bound()),
            end: map_bound(range.end_bound()),
        })
    }
}

struct DataBlockBuildState {
    written: u32,
    offset: u32,
    filter_builder: BloomFilterBuilder,
}

struct FileState {
    fd: fs::File,
    writer: BufWriter<fs::File>,
    written: u32,
}

pub(crate) struct SstWriter {
    opt: Arc<Options>,
    index_block: IndexBlock,
    filter_block: FilterBlock,
    cur_data_block: Option<DataBlockBuildState>,
    prev_data_block_end_key: Option<InternalKey<'static>>,
    file: FileState,
    internal_key_buf: Vec<u8>,
}

impl SstWriter {
    pub fn new(opt: Arc<Options>, id: u32) -> Result<Self> {
        let path = sst_path(&opt.dir, id);
        let file = fs::OpenOptions::new().create(true).write(true).open(path)?;
        Ok(Self {
            opt,
            file: FileState {
                fd: file.try_clone()?,
                writer: BufWriter::new(file),
                written: 0,
            },
            cur_data_block: None,
            index_block: Default::default(),
            filter_block: Default::default(),
            prev_data_block_end_key: None,
            internal_key_buf: vec![],
        })
    }

    #[inline(always)]
    fn with_writer_checksum<F>(file: &mut FileState, crc: u32, f: F) -> Result<(u32, BlockHandle)>
    where
        F: FnOnce(&mut dyn Write) -> Result<()>,
    {
        let mut new_crc = 0;
        let handle = Self::with_writer(file, |w| {
            let mut crc_writer = Crc32Write::new_with_initial(w, crc);
            f(&mut crc_writer)?;
            new_crc = crc_writer.finalize();
            Ok(())
        })?;
        Ok((new_crc, handle))
    }

    #[inline(always)]
    fn with_writer<F>(file: &mut FileState, f: F) -> Result<BlockHandle>
    where
        F: FnOnce(&mut dyn Write) -> Result<()>,
    {
        let offset = file.written;
        let mut byte_counter = ByteCountedWrite::new(&mut file.writer);
        f(&mut byte_counter)?;
        let size = u32::try_from(byte_counter.bytes_written()).expect("too large");
        file.written += size;

        Ok(BlockHandle { offset, size })
    }

    /// Finish a data block when there exists an active data block, and its size
    /// is larger or equal to `DATA_BLOCK_SIZE`. However, if `force` is set, the
    /// size constraint is ignored.
    fn maybe_finish_data_block(&mut self, force: bool) -> Result<()> {
        if let Some(cur_data_block) = &self.cur_data_block {
            if force || cur_data_block.written >= DATA_BLOCK_SIZE {
                // TODO: update last key of data block

                // Write data block footer.
                Self::with_writer(&mut self.file, {
                    let compression = self.opt.compression;
                    // TODO: support compression.
                    // TODO: support checksum.
                    assert!(compression == CompressionType::NoCompression);
                    move |w| DataBlockFooter { compression }.serialize_into(w)
                })?;

                let cur_data_block = self.cur_data_block.take().unwrap();

                let handle = BlockHandle {
                    offset: cur_data_block.offset,
                    size: cur_data_block.written,
                };

                // NOTE: index_block.start_keys is pushed earlier (at the time
                // the data block is started in push()).
                self.index_block.data_handles.push(handle);

                self.filter_block
                    .blooms
                    .push(cur_data_block.filter_builder.build());
            }
        }

        Ok(())
    }

    pub fn push(&mut self, rec: SstRecord<'_>) -> Result<()> {
        let internal_key = rec.internal_key(&mut self.internal_key_buf);

        // Start a data block if not present.
        if self.cur_data_block.is_none() {
            // If there exists a previous data block, we shorten (i.e. removing
            // suffix) the start key of current data block, making it only
            // sufficient to perform indexing.
            let short_key = if let Some(pkey) = self.prev_data_block_end_key.take() {
                ShortInternalKey::from_internal_shorten(&internal_key, &pkey)
            } else {
                ShortInternalKey::from_internal(&internal_key)
            };

            self.index_block.start_keys.push(short_key);

            self.cur_data_block = Some(DataBlockBuildState {
                offset: self.file.written,
                written: 0,
                filter_builder: BloomFilterBuilder::new(self.opt.bloom_bits_per_key),
            });
        }

        let handle = Self::with_writer(&mut self.file, |w| {
            todo!()
        })?;

        let cur_data_block = self.cur_data_block.as_mut().unwrap();
        cur_data_block.written += handle.size;
        cur_data_block.filter_builder.add_key(rec.key.deref());

        self.maybe_finish_data_block(false)?;

        Ok(())
    }

    /// Write the SST onto disk. When the function returns, file content is
    /// fully persisted on disk. However, the directory holding the file may not
    /// be persisted yet.
    pub fn write(mut self) -> Result<()> {
        self.maybe_finish_data_block(true)?;

        let filter_handle = Self::with_writer(&mut self.file, {
            let blk = &self.filter_block;
            move |w| serialization::serialize_into(w, blk)
        })?;

        let index_handle = Self::with_writer(&mut self.file, {
            let blk = &self.index_block;
            move |w| serialization::serialize_into(w, blk)
        })?;

        let stat_handle = {
            // TODO: stat
            let stat = StatBlock {};
            Self::with_writer(&mut self.file, |w| serialization::serialize_into(w, &stat))?
        };

        // Write footer.
        Self::with_writer(&mut self.file, |w| {
            Footer {
                filter_handle,
                index_handle,
                stat_handle,
            }
            .serialize_into(w)
        })?;

        self.file.writer.flush()?;
        self.file.fd.sync_all()?;

        Ok(())
    }
}
