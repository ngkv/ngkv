use std::{marker::PhantomData, ops::Range, path::Path, todo};

use crate::{KvOp, Lsn, Result};

// Single record.
pub struct DataItem {
    lsn: Lsn,
    op: KvOp,
}

struct BlockHandle {
    offset: u32,
    size: u32,
}

#[repr(u8)]
enum DataBlockType {
    Uncompressed,
    // TODO: compressed
}

// A data block holds multiple records.
struct DataBlock {
    typ: DataBlockType,
    recs: Vec<DataItem>,
}

#[repr(u8)]
enum MetaBlockType {
    Filter,
}

struct MetaBlock {}

pub struct SstRangeIter<'a> {
    _a: PhantomData<&'a ()>,
}

impl Iterator for SstRangeIter<'_> {
    type Item = Result<DataItem>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl DoubleEndedIterator for SstRangeIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

pub struct Sst {}

impl Sst {
    pub fn open(dir: &Path, id: u32) -> Sst {
        todo!()
    }

    pub fn get(&self, key: &[u8]) -> Result<DataItem> {
        todo!()
    }

    pub fn range(&self, key_range: (&[u8], &[u8])) -> Result<SstRangeIter<'_>> {
        todo!()
    }
}
