use std::{
    borrow::Cow,
    cmp::min,
    convert::TryInto,
    fs,
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    marker::PhantomData,
    ops::{Bound, Deref, Index, RangeBounds},
    path::{Path, PathBuf},
    sync::Arc,
    todo,
};

use crate::{
    lsm_kv::{
        serialization, BloomFilter, BloomFilterBuilder, ByteCountedRead, ByteCountedWrite,
        CompressionType, Options,
    },
    Error, Lsn, Result,
};

use bincode::config;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use chrono::format::InternalFixed;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serialization::{deserialize_from, FixedSizeSerializable};
use stdx::{binary_search::BinarySearch, bound::BoundExt, crc32_io::Crc32Write, parallel_io};

use super::DataBlockCache;

fn sst_path(dir: &Path, id: u32) -> PathBuf {
    PathBuf::from(dir).join(format!("sst-{}", id))
}

fn common_prefix_length(v1: &[u8], v2: &[u8]) -> usize {
    let min_len = min(v1.len(), v2.len());
    for i in 0..min_len {
        if v1[i] != v2[i] {
            return i;
        }
    }
    return min_len;
}

// Single record.
// #[derive(Clone)]
pub struct SstRecord {
    internal_key: InternalKey<'static>,
    value: Vec<u8>,
}

#[derive(PartialEq, Eq, Ord, Clone)]
struct InternalKey<'a>(Cow<'a, [u8]>);

impl PartialOrd for InternalKey<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        todo!()
    }
}

// impl PartialEq for InternalKey<'_> {
//     fn eq(&self, other: &Self) -> bool {
//         self.0.deref().eq(other.0.deref())
//     }
// }

impl<'a> InternalKey<'a> {
    fn new(buf: &'a [u8]) -> InternalKey<'a> {
        Self(Cow::Borrowed(buf))
    }

    fn new_owned(buf: Vec<u8>) -> InternalKey<'static> {
        InternalKey(Cow::Owned(buf))
    }

    fn apply_delta(&mut self, shared: u32, delta: &[u8]) {
        let buf = self.0.to_mut();
        buf.resize(shared as usize, 0);
        buf.extend_from_slice(delta);
    }

    fn key(&self) -> &[u8] {
        &self.0[..self.0.len() - 8]
    }

    fn lsn(&self) -> Lsn {
        let mut slice = &self.0[self.0.len() - 8..];
        slice.read_u64::<LittleEndian>().unwrap()
    }

    fn buf(&self) -> &[u8] {
        &self.0
    }

    // fn buf_mut(&mut self) -> &mut Vec<u8> {
    //     self.0.to_mut()
    // }
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

impl BlockHandle {
    fn from_written(w: Written) -> Self {
        Self {
            offset: w.offset,
            size: w.size,
        }
    }
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

const BLOCK_HANDLE_MAX_SERIALIZED_SIZE: u64 = 10;

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

struct DataBlockUncompressed {
    footer: DataBlockFooter,
    uncompressed_buf: Vec<u8>,
}

struct RecordView<'a> {
    ikey: InternalKey<'static>,
    value: &'a [u8],
    next: u32,
}

impl<'a> RecordView<'a> {
    fn from_delta_restart(delta: RecordDeltaView<'a>) -> Self {
        assert_eq!(delta.ikey_shared_len, 0, "not a restart point");
        Self {
            ikey: InternalKey::new_owned(delta.ikey_delta.to_owned()),
            value: delta.value,
            next: delta.next,
        }
    }
}

struct RecordDeltaView<'a> {
    ikey_shared_len: u32,
    ikey_delta: &'a [u8],
    value: &'a [u8],
    next: u32,
}

struct RestartView<'a> {
    block: &'a DataBlockUncompressed,
}

impl RestartView<'_> {
    fn new<'a>(block: &'a DataBlockUncompressed) -> RestartView<'a> {
        RestartView { block }
    }

    fn point_buf(&self) -> &[u8] {
        &self.block.uncompressed_buf[self.block.footer.restart_array_offset as usize..]
    }

    fn point_count(&self) -> u32 {
        self.point_buf().len() as u32 / 4
    }

    fn point_get(&self, idx: u32) -> u32 {
        let mut repr = [0u8; 4];
        let start = (idx * 4) as usize;
        repr.clone_from_slice(&self.point_buf()[start..start + 4]);
        u32::from_le_bytes(repr)
    }
}

impl<'a> BinarySearch for RestartView<'a> {
    type Elem = InternalKey<'a>;

    fn elem_count(&self) -> usize {
        self.point_count() as usize
    }

    fn elem(&self, idx: usize) -> Self::Elem {
        let point = self.point_get(idx as u32);
        let delta = self.block.delta_at(point);
        assert_eq!(delta.ikey_shared_len, 0);
        InternalKey::new(delta.ikey_delta)
    }
}

impl DataBlockUncompressed {
    fn delta_at(&self, offset: u32) -> RecordDeltaView<'_> {
        assert!(offset < self.footer.restart_array_offset);

        let mut slice =
            &self.uncompressed_buf[offset as usize..self.footer.restart_array_offset as usize];
        let len = slice.len();

        let ikey_shared_len: u32 = serialization::deserialize_from(&mut slice).unwrap();
        let ikey_delta_len: u32 = serialization::deserialize_from(&mut slice).unwrap();
        let value_len: u32 = serialization::deserialize_from(&mut slice).unwrap();

        let rem = slice;
        let (ikey_delta, rem) = rem.split_at(ikey_delta_len as usize);
        let (value, rem) = rem.split_at(value_len as usize);

        let bytes_read = (len - rem.len()) as u32;

        RecordDeltaView {
            ikey_shared_len,
            ikey_delta,
            value,
            next: offset + bytes_read,
        }
    }

    fn prev_restart_point(&self, ik: &InternalKey<'_>) -> Option<u32> {
        let restart_array = RestartView::new(&self);
        restart_array
            .binary_search(ik)
            .map_or_else(|ins| ins.checked_sub(1), |idx| Some(idx))
            .map(|idx| idx as u32)
    }

    // fn record_at(&self, offset: u32) -> RecordView<'_> {
    //     let restart_array = RestartView::new(
    //         &self.uncompressed_buf[self.footer.restart_array_offset as usize..],
    //     );

    //     // Find first restart point with less or equal offset.
    //     let idx = restart_array
    //         .binary_search(offset)
    //         .map_or_else(|ins| ins.checked_sub(1).unwrap(), |idx| idx) as u32;

    //     let restart_point = restart_array.get(idx);

    //     // Read record at restart point.
    //     let delta = self.delta_at(restart_point);
    //     assert_eq!(delta.ikey_shared_len, 0);

    //     let mut cur_rec = RecordView {
    //         ikey: InternalKey::new_owned(delta.ikey_delta.to_owned()),
    //         value: delta.value,
    //         next: delta.next,
    //     };
    //     let mut cur_offset = restart_point;

    //     while cur_offset < offset {
    //         // Advance to next record.
    //         let (offset, rec) = self
    //             .next_record(cur_rec)
    //             .expect("unexpected end of records");
    //         cur_offset = offset;
    //         cur_rec = rec;
    //     }

    //     assert_eq!(cur_offset, offset);

    //     cur_rec
    // }

    // fn next_record(&self, cur: RecordView<'_>) -> Option<(u32, RecordView<'_>)> {
    //     if cur.next == self.footer.restart_array_offset {
    //         None
    //     } else {
    //         let delta = self.delta_at(cur.next);
    //         let mut ikey = cur.ikey;
    //         ikey.apply_delta(delta.ikey_shared_len, delta.ikey_delta);

    //         Some((
    //             cur.next,
    //             RecordView {
    //                 ikey,
    //                 next: delta.next,
    //                 value: delta.value,
    //             },
    //         ))
    //     }
    // }
}

// TODO: make it a smart pointer, allowing cached block get.
struct DataBlockUncompressedHandle<'a> {
    data: DataBlockUncompressed,
    _t: PhantomData<&'a ()>,
}

impl Deref for DataBlockUncompressedHandle<'_> {
    type Target = DataBlockUncompressed;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

struct BlockIter<'a> {
    block_handle: DataBlockUncompressedHandle<'a>,
    start_key: Bound<InternalKey<'a>>,
    end_key: Bound<InternalKey<'a>>,
    init: bool,
    next_offset: u32,
    cur_key: InternalKey<'static>,
}

impl<'a> Iterator for BlockIter<'a> {
    type Item = SstRecord;

    fn next(&mut self) -> Option<Self::Item> {
        let block = self.block_handle.deref();

        let mut bound_check = false;

        if !self.init {
            bound_check = true;
            self.init = true;

            if let Some(ik_info) = self.start_key.info() {
                let restart_point = block
                    .prev_restart_point(ik_info.content)
                    .expect("key is not contained by this data block");

                self.next_offset = restart_point;
            }
        }

        loop {
            if self.next_offset >= self.block_handle.footer.restart_array_offset {
                return None;
            }

            let delta = block.delta_at(self.next_offset);
            self.cur_key
                .apply_delta(delta.ikey_shared_len, delta.ikey_delta);
            self.next_offset = delta.next;

            if !self.end_key.is_right_bound_of(&self.cur_key) {
                return None;
            }

            if !bound_check || self.start_key.is_left_bound_of(&self.cur_key) {
                return Some(SstRecord {
                    internal_key: self.cur_key.clone(),
                    value: delta.value.to_owned(),
                });
            }
        }
    }
}

#[derive(Clone, Copy, Default)]
struct RecordPtr {
    block_idx: u32,
    in_block_offset: u32,
}

struct BlockIterWithIdx<'a> {
    block_idx: u32,
    iter: BlockIter<'a>,
}

enum LivingBlockIter<'a> {
    Seperated {
        forward: Option<BlockIterWithIdx<'a>>,
        backward: Option<BlockIterWithIdx<'a>>,
    },
    Merged(BlockIterWithIdx<'a>),
}

pub struct SstIter<'a> {
    sst: &'a Sst,
    start_key: Bound<InternalKey<'a>>,
    end_key: Bound<InternalKey<'a>>,
    iter_with_idx: Option<BlockIterWithIdx<'a>>,
    block_iters: Vec<BlockIter<'a>>,
    // forward_block_idx: Option<u32>,
    // backward_block_idx: Option<u32>,
    // living_block_iter: LivingBlockIter<'a>,
    failed: bool,
}

impl Iterator for SstIter<'_> {
    type Item = Result<SstRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }

        if self.iter_with_idx.is_none() {
            let block_idx = self.sst.seek_block_forward(&self.start_key);
            let iter =
                match self
                    .sst
                    .block_iter(block_idx, self.start_key.clone(), self.end_key.clone())
                {
                    Ok(x) => x,
                    Err(e) => {
                        self.failed = true;
                        return Some(Err(e));
                    }
                };

            self.iter_with_idx = Some(BlockIterWithIdx { block_idx, iter });
        }

        let iter_with_idx = self.iter_with_idx.as_mut().unwrap();
        match iter_with_idx.iter.next() {
            Some(x) => Some(x),
            None => {
                let next_idx = iter_with_idx.block_idx + 1;
                if next_idx < self.sst.index_block.data_handles.len() as u32 {
                    self.iter_with_idx = 

                }

                todo!()
            }
        };

        todo!()
    }
}

impl DoubleEndedIterator for SstIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

pub struct Sst {
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
    r.seek(SeekFrom::Start(handle.offset))?;
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
            data_cache: opt.data_block_cache.clone(),
            opt,
            index_block,
            filter_block,
            stat_block,
            fd,
        })
    }

    fn block_iter<'a>(
        &'a self,
        block_idx: u32,
        start: Bound<InternalKey<'a>>,
        end: Bound<InternalKey<'a>>,
    ) -> Result<BlockIter<'a>> {
        Ok(BlockIter {
            start_key: start,
            end_key: end,
            block_handle: self.data_block(block_idx)?,
            init: false,
            next_offset: 0,
            cur_key: InternalKey::new_owned(vec![]),
        })
    }

    /// Seek to the first data block which might be insteresting.
    fn seek_block_forward(&self, start: &Bound<InternalKey>) -> u32 {
        let mut included = false;
        let ik = match start {
            Bound::Included(ik) => {
                included = true;
                Some(ik)
            }
            Bound::Excluded(ik) => Some(ik),
            Bound::Unbounded => None,
        };

        if let Some(ik) = ik {
            // Find index of last key that is smaller or equal to
            // current key.
            self.index_block
                .start_keys
                .binary_search_by_key(&ik.buf(), |sk| sk.buf())
                .map_or_else(|ins| ins.saturating_sub(1), |idx| idx) as u32
        } else {
            0
        }
    }

    fn data_block(&self, idx: u32) -> Result<DataBlockUncompressedHandle<'_>> {
        // TODO: cached read
        let handle = self.index_block.data_handles[idx as usize];
        let mut buf = vec![0u8; handle.size as usize];
        parallel_io::pread_exact(&self.fd, &mut buf, handle.offset)?;

        let footer_start = buf.len() - DataBlockFooter::serialized_size();
        let footer = DataBlockFooter::deserialize_from(&buf[footer_start..])?;

        buf.resize(footer_start, 0);

        Ok(DataBlockUncompressedHandle {
            _t: PhantomData::default(),
            data: DataBlockUncompressed {
                footer,
                uncompressed_buf: buf,
            },
        })
    }

    pub fn range<'s: 'k, 'k>(&'s self, range: impl RangeBounds<&'k [u8]>) -> Result<SstIter<'k>> {
        let map_bound = |b| match b {
            Bound::Included(&t) => Bound::Included(InternalKey::new(t)),
            Bound::Excluded(&t) => Bound::Excluded(InternalKey::new(t)),
            Bound::Unbounded => Bound::Unbounded,
        };

        // Ok(SstRangeIter {
        //     sst: self,
        //     start: map_bound(range.start_bound()),
        //     end: map_bound(range.end_bound()),
        // })

        todo!()
    }
}

struct DataBlockBuildState {
    records_since_restart: u32,
    bytes_written: u32,
    bytes_offset: u64,
    filter_builder: BloomFilterBuilder,

    /// Offset of restart points, relative to the beginning of data block.
    restart_array: Vec<u32>,
}

struct FileState {
    fd: fs::File,
    writer: BufWriter<fs::File>,
    bytes_written: u64,
}

pub struct SstWriter {
    opt: Arc<Options>,
    index_block: IndexBlock,
    filter_block: FilterBlock,
    cur_data_block: Option<DataBlockBuildState>,
    prev_key: Option<InternalKey<'static>>,
    file: FileState,
    internal_key_buf: Vec<u8>,
}

struct Written {
    size: u64,
    offset: u64,
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
                bytes_written: 0,
            },
            cur_data_block: None,
            index_block: Default::default(),
            filter_block: Default::default(),
            // prev_data_block_end_key: None,
            prev_key: None,
            internal_key_buf: vec![],
        })
    }

    #[inline(always)]
    fn with_writer_checksum<F>(file: &mut FileState, crc: u32, f: F) -> Result<(u32, Written)>
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
    fn with_writer<F>(file: &mut FileState, f: F) -> Result<Written>
    where
        F: FnOnce(&mut dyn Write) -> Result<()>,
    {
        let offset = file.bytes_written;
        let mut byte_counter = ByteCountedWrite::new(&mut file.writer);
        f(&mut byte_counter)?;
        let size = byte_counter.bytes_written() as u64;
        file.bytes_written += size;

        Ok(Written { offset, size })
    }

    /// Finish a data block when there exists an active data block, and its size
    /// is larger or equal to `DATA_BLOCK_SIZE`. However, if `force` is set, the
    /// size constraint is ignored.
    fn maybe_finish_data_block(&mut self, force: bool) -> Result<()> {
        if let Some(cur_data_block) = &self.cur_data_block {
            if force || cur_data_block.bytes_written >= self.opt.data_block_size {
                let mut cur_data_block = self.cur_data_block.take().unwrap();

                // Write data block restart array & footer.
                let written = Self::with_writer(&mut self.file, {
                    // TODO: support compression.
                    // TODO: support checksum.
                    let compression = self.opt.compression;
                    assert!(compression == CompressionType::NoCompression);

                    let restart_array_offset = cur_data_block.bytes_written;
                    let restart_array = &cur_data_block.restart_array;

                    move |w| {
                        for &p in restart_array {
                            w.write_u32::<LittleEndian>(p)?;
                        }
                        let footer = DataBlockFooter {
                            compression,
                            restart_array_offset,
                        };
                        footer.serialize_into(&mut *w)?;
                        Ok(())
                    }
                })?;

                assert!(written.size <= u32::MAX as u64);
                cur_data_block.bytes_written += written.size as u32;

                let handle = BlockHandle {
                    offset: cur_data_block.bytes_offset,
                    size: cur_data_block.bytes_written as u64,
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

    // Start a data block if not present.
    fn maybe_start_data_block(&mut self, rec: &SstRecord) -> bool {
        if self.cur_data_block.is_none() {
            // If there exists a previous data block, we shorten (i.e. removing
            // suffix) the start key of current data block, making it only
            // sufficient to perform indexing.
            let short_key = ShortInternalKey::new(&rec.internal_key, self.prev_key.as_ref());

            self.index_block.start_keys.push(short_key);

            self.cur_data_block = Some(DataBlockBuildState {
                bytes_offset: self.file.bytes_written,
                bytes_written: 0,
                restart_array: vec![],
                records_since_restart: 0,
                filter_builder: BloomFilterBuilder::new(self.opt.bloom_bits_per_key),
            });

            true
        } else {
            false
        }
    }

    pub fn push(&mut self, rec: &SstRecord) -> Result<()> {
        let mut restart = false;

        if self.maybe_start_data_block(rec) {
            restart = true;
        }

        let cur_data_block = self.cur_data_block.as_mut().unwrap();

        // Add key to bloom filter.
        cur_data_block
            .filter_builder
            .add_key(rec.internal_key.key());

        if cur_data_block.records_since_restart >= self.opt.data_block_restart_interval {
            restart = true;
        }

        let ikey_buf = rec.internal_key.buf();
        let ikey_shared_len: u32;
        let ikey_delta: &[u8];

        // Perform common prefix removal.
        if restart {
            cur_data_block.records_since_restart = 0;
            // TODO: insert restart point to block footer
            ikey_shared_len = 0;
            ikey_delta = ikey_buf;
        } else {
            cur_data_block.records_since_restart += 1;
            let prev_key = self.prev_key.as_ref().unwrap();
            let prefix_len = common_prefix_length(&rec.internal_key.buf(), prev_key.buf());
            ikey_shared_len = prefix_len as u32;
            ikey_delta = &ikey_buf[prefix_len..];
        }

        // Update prev_key.
        if let Some(prev_key) = &mut self.prev_key {
            prev_key.apply_delta(ikey_shared_len as u32, ikey_delta);
        } else {
            self.prev_key = Some(rec.internal_key.clone());
        }

        // Write current record.
        let handle = Self::with_writer(&mut self.file, {
            let mut ikey_delta = ikey_delta;
            let ikey_delta_len: u32 = ikey_delta.len() as u32;
            let mut value = rec.value.deref();
            let value_len: u32 = value.len() as u32;

            move |w| {
                serialization::serialize_into(&mut *w, &ikey_shared_len)?;
                serialization::serialize_into(&mut *w, &ikey_delta_len)?;
                serialization::serialize_into(&mut *w, &value_len)?;
                io::copy(&mut ikey_delta, &mut *w)?;
                io::copy(&mut value, &mut *w)?;
                Ok(())
            }
        })?;

        cur_data_block.bytes_written = cur_data_block
            .bytes_written
            .checked_add(handle.size as u32)
            .expect("data block too large");

        self.maybe_finish_data_block(false)?;
        Ok(())
    }

    /// Write the SST onto disk. When the function returns, file content is
    /// fully persisted on disk. However, the directory holding the file may not
    /// be persisted yet.
    pub fn write(mut self) -> Result<()> {
        self.maybe_finish_data_block(true)?;

        let filter_handle = BlockHandle::from_written(Self::with_writer(&mut self.file, {
            let blk = &self.filter_block;
            move |w| serialization::serialize_into(w, blk)
        })?);

        let index_handle = BlockHandle::from_written(Self::with_writer(&mut self.file, {
            let blk = &self.index_block;
            move |w| serialization::serialize_into(w, blk)
        })?);

        let stat_handle = BlockHandle::from_written({
            // TODO: stat
            let stat = StatBlock {};
            Self::with_writer(&mut self.file, |w| serialization::serialize_into(w, &stat))?
        });

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
