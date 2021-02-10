mod bloom;
mod byte_counter;
mod serialization;
mod sst;
mod version_set;

use bloom::*;
use byte_counter::*;

use std::{
    borrow::Cow,
    io::Cursor,
    ops::Deref,
    path::PathBuf,
    sync::{Arc, Mutex},
    todo,
};

use crate::{Error, Lsn, Result, ShouldRun, Task, TaskCtl};

use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use wal_fsm::{Fsm, FsmOp, WalFsm};

pub(crate) const LEVEL_COUNT: u32 = 6;

#[repr(u8)]
#[derive(Copy, Clone, PartialEq, Eq)]
pub enum CompressionType {
    NoCompression,
    // TODO: compressed
}

// TODO: data block cache
pub struct DataBlockCache {}

pub struct Options {
    pub dir: PathBuf,
    pub bloom_bits_per_key: u32,
    pub compression: CompressionType,
    pub data_block_cache: Option<Arc<DataBlockCache>>,
    pub data_block_size: u64,
    pub data_block_restart_interval: u32,
}

struct KvFsmOp {
    op: KvOp<'static>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum ValueOp<'a> {
    Put { value: Cow<'a, [u8]> },
    Delete,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct KvOp<'a> {
    key: Cow<'a, [u8]>,
    value: ValueOp<'a>,
}

impl KvOp<'_> {
    pub fn key(&self) -> &[u8] {
        self.key.deref()
    }

    // pub fn serialize_into(&self, mut w: impl Write) -> Result<()> {
    //     match &self {
    //         KvOp::Put { key, value } => {
    //             w.write_u8(OpType::Put as u8)?;
    //             serialize_buf(&mut w, key)?;
    //             serialize_buf(&mut w, value)?;
    //         }
    //         KvOp::Delete { key } => {
    //             w.write_u8(OpType::Delete as u8)?;
    //             serialize_buf(&mut w, key)?;
    //         }
    //     };

    //     Ok(())
    // }

    // pub fn deserialize_from(mut r: impl Read) -> Result<Self> {
    //     let typ = r.read_u8()?;
    //     match typ {
    //         x if x == OpType::Put as u8 => {
    //             let key = deserialize_buf(&mut r)?;
    //             let value = deserialize_buf(&mut r)?;
    //             Ok(KvOp::Put { key: Cow::Owned(key), value: Cow::Owned(value) })
    //         }
    //         x if x == OpType::Delete as u8 => {
    //             let key = deserialize_buf(&mut r)?;
    //             Ok(KvOp::Delete { key })
    //         }
    //         _ => Err(Error::Corrupted("invalid type".into())),
    //     }
    // }
}

#[repr(u8)]
enum OpType {
    Put = 1,
    Delete = 2,
}

// fn serialize_buf(mut w: impl Write, buf: &[u8]) -> io::Result<()> {
//     let len = u32::try_from(buf.len()).expect("buf too large");
//     w.write_var_u32(len)?;
//     w.write_all(buf)?;
//     Ok(())
// }

// fn deserialize_buf(mut r: impl Read) -> io::Result<Vec<u8>> {
//     let len = r.read_var_u32()?;
//     let mut buf = vec![0u8; len as usize];
//     r.read_exact(&mut buf)?;
//     Ok(buf)
// }

impl FsmOp for KvFsmOp {
    type E = Error;

    fn serialize(&self) -> Result<Vec<u8>> {
        let mut buf = vec![];
        serialization::serialize_into(&mut buf, &self.op).unwrap();
        Ok(buf)
    }

    fn deserialize(buf: &[u8]) -> Result<Self> {
        let op = serialization::deserialize_from(buf).unwrap();
        Ok(KvFsmOp { op })
    }
}

struct KvFsmState {}

struct KvFsmShared {
    state: Mutex<KvFsmState>,
}

struct CompactionTask {
    sink: Box<dyn wal_fsm::ReportSink>,
}

impl Task for CompactionTask {
    type Error = Error;

    fn should_run(&mut self) -> ShouldRun {
        // TODO: Check following conditions:
        // 1. Memtable reach certain size (minor compaction).
        // 2.
        todo!()
    }

    fn run(&mut self) -> Result<(), Self::Error> {
        todo!()
    }
}

struct KvFsm {
    shared: Arc<KvFsmShared>,
    compact_ctl: OnceCell<TaskCtl<CompactionTask>>,
}

impl Fsm for KvFsm {
    type Op = KvFsmOp;
    type E = Error;

    fn init(&self, sink: Box<dyn wal_fsm::ReportSink>) -> Result<wal_fsm::Init> {
        // TODO:
        // 1. Initialize version set, read latest SST to get next LSN (for
        //    recovery).
        // 2. Initialize compaction task.
        todo!()
    }

    fn apply(&self, _op: Self::Op, _lsn: Lsn) -> Result<()> {
        // TODO: Write record into mem table. Suspend if mem table is full & imm
        // table is flushing.
        todo!()
    }
}

pub struct LsmKv {
    _wal_fsm: WalFsm<KvFsm>,
}
