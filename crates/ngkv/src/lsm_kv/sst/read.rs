use std::{
    fs,
    io::{BufReader, Read, Seek, SeekFrom},
    marker::PhantomData,
    ops::{Bound, Deref},
    sync::Arc,
};

use crate::{
    lsm_kv::{
        serialization,
        sst::{
            sst_path, BlockHandle, DataBlockFooter, FilterBlock, Footer, IndexBlock, InternalKey,
            SstRecord, StatBlock,
        },
        ByteCountedRead, DataBlockCache, Options,
    },
    Error, Result,
};

use serde::de::DeserializeOwned;
use serialization::{deserialize_from, FixedSizeSerializable};
use stdx::{binary_search::BinarySearch, bound::BoundExt, parallel_io};

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
        InternalKey::from_buf(delta.ikey_delta)
    }
}

struct DataBlockUncompressed {
    footer: DataBlockFooter,
    uncompressed_buf: Vec<u8>,
}

// struct RecordView<'a> {
//     ikey: InternalKey<'static>,
//     value: &'a [u8],
//     next: u32,
// }

// impl<'a> RecordView<'a> {
//     fn from_delta_restart(delta: RecordDeltaView<'a>) -> Self {
//         assert_eq!(delta.ikey_shared_len, 0, "not a restart point");
//         Self {
//             ikey: InternalKey::new_owned(delta.ikey_delta.to_owned()),
//             value: delta.value,
//             next: delta.next,
//         }
//     }
// }

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

    fn prev_restart_point(&self, ik: &InternalKey<'_>) -> u32 {
        let restart_array = RestartView::new(&self);
        restart_array
            .binary_search(ik)
            .map_or_else(|ins| ins, |idx| idx) as u32
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

struct DataBlockIter<'a> {
    sst: &'a Sst,
    block_idx: u32,
    block_handle: Option<DataBlockUncompressedHandle<'a>>,
    start_key: Bound<InternalKey<'a>>,
    end_key: Bound<InternalKey<'a>>,
    init: bool,
    next_offset: u32,
    cur_key: InternalKey<'static>,
}

impl<'a> Iterator for DataBlockIter<'a> {
    type Item = Result<SstRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.block_handle.is_none() {
            let block_handle = match self.sst.data_block_handle(self.block_idx) {
                Ok(x) => x,
                Err(e) => return Some(Err(e)),
            };
            self.block_handle = Some(block_handle);
        };

        let block = self.block_handle.as_deref().unwrap();
        let mut bound_check = false;

        if !self.init {
            bound_check = true;
            self.init = true;

            if let Some(ik_info) = self.start_key.info() {
                let restart_point = block.prev_restart_point(ik_info.content);
                self.next_offset = restart_point;
            }
        }

        loop {
            if self.next_offset >= block.footer.restart_array_offset {
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
                return Some(Ok(SstRecord {
                    internal_key: self.cur_key.clone(),
                    value: delta.value.to_owned(),
                }));
            }
        }
    }
}

// pub struct SstIter<'a> {
//     sst: &'a Sst,
//     start_key: Bound<InternalKey<'a>>,
//     end_key: Bound<InternalKey<'a>>,
//     iter_with_idx: Option<BlockIterWithIdx<'a>>,
//     block_iters: Vec<DataBlockIter<'a>>,
//     // forward_block_idx: Option<u32>,
//     // backward_block_idx: Option<u32>,
//     // living_block_iter: LivingBlockIter<'a>,
//     failed: bool,
// }

// impl Iterator for SstIter<'_> {
//     type Item = Result<SstRecord>;

//     fn next(&mut self) -> Option<Self::Item> {
//         if self.failed {
//             return None;
//         }

//         if self.iter_with_idx.is_none() {
//             let block_idx = self.sst.seek_block_forward(&self.start_key);
//             let iter = match self.sst.data_block_iter(
//                 block_idx,
//                 self.start_key.clone(),
//                 self.end_key.clone(),
//             ) {
//                 Ok(x) => x,
//                 Err(e) => {
//                     self.failed = true;
//                     return Some(Err(e));
//                 }
//             };

//             self.iter_with_idx = Some(BlockIterWithIdx { block_idx, iter });
//         }

//         let iter_with_idx = self.iter_with_idx.as_mut().unwrap();
//         match iter_with_idx.iter.next() {
//             Some(x) => Some(x),
//             None => {
//                 let next_idx = iter_with_idx.block_idx + 1;
//                 if next_idx < self.sst.index_block.data_handles.len() as u32 {
//                     // self.iter_with_idx =
//                 }

//                 todo!()
//             }
//         };

//         todo!()
//     }
// }

pub struct Sst {
    opt: Arc<Options>,
    data_cache: Option<Arc<DataBlockCache>>,
    index_block: IndexBlock,
    filter_block: FilterBlock,
    stat_block: StatBlock,
    fd: fs::File,
}

impl Sst {
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

    pub fn open(opt: Arc<Options>, id: u32) -> Result<Sst> {
        let path = sst_path(&opt.dir, id);
        let mut fd = fs::OpenOptions::new().read(true).open(path)?;
        let mut reader = BufReader::new(&mut fd);

        reader.seek(SeekFrom::End(-(Footer::serialized_size() as i64)))?;
        let footer = Footer::deserialize_from(&mut reader)?;

        let index_block = Self::read_deser_block(&mut reader, footer.index_handle)?;
        let filter_block = Self::read_deser_block(&mut reader, footer.filter_handle)?;
        let stat_block = Self::read_deser_block(&mut reader, footer.stat_handle)?;

        Ok(Sst {
            data_cache: opt.data_block_cache.clone(),
            opt,
            index_block,
            filter_block,
            stat_block,
            fd,
        })
    }

    fn data_block_iter<'a>(
        &'a self,
        block_idx: u32,
        start: Bound<InternalKey<'a>>,
        end: Bound<InternalKey<'a>>,
    ) -> Result<DataBlockIter<'a>> {
        Ok(DataBlockIter {
            sst: self,
            block_idx,
            start_key: start,
            end_key: end,
            block_handle: None,
            init: false,
            next_offset: 0,
            cur_key: InternalKey::from_owned_buf(vec![]),
        })
    }

    fn data_block_handle(&self, idx: u32) -> Result<DataBlockUncompressedHandle<'_>> {
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

    fn borrow_key_bound<'k>(b: &'k Bound<InternalKey>) -> Bound<InternalKey<'k>> {
        match b {
            Bound::Included(ik) => Bound::Included(InternalKey::from_buf(ik.buf())),
            Bound::Excluded(ik) => Bound::Excluded(InternalKey::from_buf(ik.buf())),
            Bound::Unbounded => Bound::Unbounded,
        }
    }

    pub fn range<'a>(
        &'a self,
        start: &'a Bound<InternalKey<'_>>,
        end: &'a Bound<InternalKey<'_>>,
    ) -> Result<Box<dyn 'a + Iterator<Item = Result<SstRecord>>>> {
        let idx_begin = if let Some(ik_info) = start.info() {
            self.index_block
                .start_keys
                .binary_search_by_key(&ik_info.content.buf(), |sk| sk.buf())
                .map_or_else(|ins| ins, |idx| idx) as u32
        } else {
            0
        };

        let idx_end = if let Some(ik_info) = end.info() {
            self.index_block
                .start_keys
                .binary_search_by_key(&ik_info.content.buf(), |sk| sk.buf())
                .map_or_else(|ins| ins.saturating_sub(1), |idx| idx) as u32
        } else {
            self.index_block.start_keys.len() as u32 - 1
        };

        assert!(idx_end >= idx_begin);

        let mut block_iters = vec![];
        for idx in idx_begin..=idx_end {
            let start = if idx == idx_begin {
                Self::borrow_key_bound(&start)
            } else {
                Bound::Unbounded
            };

            let end = if idx == idx_end {
                Self::borrow_key_bound(&end)
            } else {
                Bound::Unbounded
            };

            let iter = self.data_block_iter(idx as u32, start, end)?;
            block_iters.push(iter);
        }

        Ok(Box::new(block_iters.into_iter().flat_map(|iter| iter)))
    }
}
