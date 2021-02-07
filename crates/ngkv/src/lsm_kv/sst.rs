use std::{marker::PhantomData, path::Path, io::Write, todo};

use crate::{KvOp, Lsn, Result};

use serde::{Serialize, Deserialize};

// Single record.
#[derive(Serialize, Deserialize)]
pub struct SstRecord {
    lsn: Lsn,
    op: KvOp,
}

// Borrowed version of single record. Only for lookup usage.
#[derive(Serialize, Deserialize)]
struct SstRecordBorrowed<'a> {
    lsn: Lsn,


}

#[derive(Serialize, Deserialize)]
struct BlockHandle {
    offset: u32,
    size: u32,
}

const HEADER_MAGIC:

#[derive(Serialize, Deserialize)]
struct Header {
    magic: u64,
}

#[repr(u8)]
enum DataBlockType {
    Uncompressed,
    // TODO: compressed
}

// A data block holds multiple records.
struct DataBlock {
    typ: DataBlockType,
    recs: Vec<SstRecord>,
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
    type Item = Result<SstRecord>;

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

    pub fn get(&self, key: &[u8]) -> Result<SstRecord> {
        todo!()
    }

    pub fn range(&self, key_range: (&[u8], &[u8])) -> Result<SstRangeIter<'_>> {
        todo!()
    }
}

pub struct SstBuilder {}

impl SstBuilder {
    pub fn build(mut writer: impl Write, records: impl Iterator<Item = SstRecord>) -> Result<()> {
        todo!()
    }
}
