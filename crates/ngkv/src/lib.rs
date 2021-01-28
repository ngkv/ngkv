mod mem_kv;
mod varint;
mod cow_arc;

use std::{any::TypeId, convert::TryFrom, io, marker::PhantomData, ops::Bound, todo};

pub use crate::{varint::*, cow_arc::*};
use byteorder::{ReadBytesExt, WriteBytesExt};
use io::{Cursor, Read, Write};
use thiserror::Error;
use wal_fsm::{FileLogOptions, Fsm, FsmOp, LogCtx, WalFsm};

struct KvFsm {}

#[derive(Clone)]
enum KvFsmOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

#[repr(u8)]
enum OpType {
    Put = 1,
    Delete = 2,
}

fn serialize_buf(mut w: impl Write, buf: &[u8]) -> io::Result<()> {
    let len = u32::try_from(buf.len()).expect("buf too large");
    w.write_var_u32(len)?;
    w.write_all(buf)?;
    Ok(())
}

fn deserialize_buf(mut r: impl Read) -> io::Result<Vec<u8>> {
    let len = r.read_var_u32()?;
    let mut buf = vec![0u8; len as usize];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

impl FsmOp for KvFsmOp {
    fn serialize(&self) -> wal_fsm::Result<Vec<u8>> {
        let mut cursor = Cursor::new(vec![]);
        match self {
            KvFsmOp::Put { key, value } => {
                cursor.write_u8(OpType::Put as u8)?;
                serialize_buf(&mut cursor, key)?;
                serialize_buf(&mut cursor, value)?;
            }
            KvFsmOp::Delete { key } => {
                cursor.write_u8(OpType::Delete as u8)?;
                serialize_buf(&mut cursor, key)?;
            }
        };

        Ok(cursor.into_inner())
    }

    fn deserialize(buf: &[u8]) -> wal_fsm::Result<Self> {
        let mut cursor = Cursor::new(buf);
        let typ = cursor.read_u8()?;
        match typ {
            x if x == OpType::Put as u8 => {
                let key = deserialize_buf(&mut cursor)?;
                let value = deserialize_buf(&mut cursor)?;
                Ok(Self::Put { key, value })
            }
            x if x == OpType::Delete as u8 => {
                let key = deserialize_buf(&mut cursor)?;
                Ok(Self::Delete { key })
            }
            _ => Err(wal_fsm::Error::Corrupted("invalid type".into())),
        }
    }
}

impl Fsm for KvFsm {
    type Op = KvFsmOp;

    fn init(&self, sink: Box<dyn wal_fsm::ReportSink>) -> wal_fsm::Init {
        todo!()
    }

    fn apply(&self, op: Self::Op, lsn: wal_fsm::Lsn) {
        todo!()
    }
}

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

// pub enum KvOp<'a> {
//     Put { key: &'a [u8], value: &'a [u8] },
//     Delete { key: &'a [u8] },
// }

pub struct WriteOptions {
    is_sync: bool,
}

// pub struct Snapshot<'a> {
//     _s: PhantomData<&'a ()>,
// }

pub struct ReadOptions<'a> {
    snapshot: Option<&'a dyn Snapshot>,
}

struct LsmKv {
    wal_fsm: WalFsm<KvFsm>,
}

pub enum CompareAndSwapStatus {
    Succeeded,
    CurrentMismatch { cur_value: Option<Vec<u8>> },
}

pub struct RangeBound {
    pub start: Bound<Vec<u8>>,
    pub end: Bound<Vec<u8>>,
}

pub trait RangeIterator: Iterator<Item = (Vec<u8>, Vec<u8>)> {}

pub trait Snapshot {
}

pub trait Kv {
    fn put(&self, options: &WriteOptions, key: &[u8], value: &[u8]);

    fn delete(&self, options: &WriteOptions, key: &[u8]);

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
        range: RangeBound,
    ) -> Result<Box<dyn '_ + RangeIterator>>;
}