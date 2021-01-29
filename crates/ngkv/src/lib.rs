mod cow_arc;
mod lsm_kv;
mod mem_kv;
mod varint;

use std::{
    io,
    ops::{Bound, RangeBounds},
};

pub use crate::{cow_arc::*, lsm_kv::*, varint::*};
use thiserror::Error;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("reportable bug: {0}")]
    ReportableBug(String),
    #[error("data corrupted: {0}")]
    Corrupted(String),
    #[error(transparent)]
    Wal(#[from] wal_fsm::Error),
}

#[derive(Default)]
pub struct WriteOptions {
    pub is_sync: bool,
}

#[derive(Default)]
pub struct ReadOptions<'a> {
    snapshot: Option<&'a dyn Snapshot>,
}

pub enum CompareAndSwapStatus {
    Succeeded,
    CurrentMismatch { cur_value: Option<Vec<u8>> },
}

struct RangeBoundImpl {
    start: Bound<Vec<u8>>,
    end: Bound<Vec<u8>>,
}

pub struct Kvp {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

pub trait RangeIterator: DoubleEndedIterator<Item = Result<Kvp>> {}

pub trait Snapshot {}

pub trait Kv {
    fn put(&self, options: &WriteOptions, key: &[u8], value: &[u8]) -> Result<()>;

    fn delete(&self, options: &WriteOptions, key: &[u8]) -> Result<()>;

    fn compare_and_swap(
        &self,
        options: &WriteOptions,
        key: &[u8],
        cur_value: Option<&[u8]>,
        new_value: Option<&[u8]>,
    ) -> Result<CompareAndSwapStatus>;

    fn snapshot(&self) -> Box<dyn '_ + Snapshot>;

    fn get(&self, options: &ReadOptions<'_>, key: &[u8]) -> Result<Option<Vec<u8>>>;

    fn range(
        &self,
        options: &ReadOptions<'_>,
        range: impl RangeBounds<Vec<u8>>,
    ) -> Result<Box<dyn '_ + RangeIterator>>;
}
