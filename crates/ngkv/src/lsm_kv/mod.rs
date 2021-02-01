mod sst;
mod version_set;

// use version_set::*;

use std::{
    convert::TryFrom,
    io::{self, Cursor, Read, Write},
    sync::{Arc, Mutex},
    todo,
};

use byteorder::{ReadBytesExt, WriteBytesExt};
use once_cell::sync::OnceCell;
use wal_fsm::{Fsm, FsmOp, WalFsm};

use crate::{Error, Lsn, Result, ShouldRun, Task, TaskCtl, VarintRead, VarintWrite};

struct KvFsmOp {
    op: KvOp,
}

#[derive(Clone)]
pub(crate) enum KvOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl KvOp {
    pub fn key(&self) -> &[u8] {
        match &self {
            KvOp::Put { key, .. } => key,
            KvOp::Delete { key, .. } => key,
        }
    }

    pub fn serialize_into(&self, mut w: impl Write) -> Result<()> {
        match &self {
            KvOp::Put { key, value } => {
                w.write_u8(OpType::Put as u8)?;
                serialize_buf(&mut w, key)?;
                serialize_buf(&mut w, value)?;
            }
            KvOp::Delete { key } => {
                w.write_u8(OpType::Delete as u8)?;
                serialize_buf(&mut w, key)?;
            }
        };

        Ok(())
    }

    pub fn deserialize_from(mut r: impl Read) -> Result<Self> {
        let typ = r.read_u8()?;
        match typ {
            x if x == OpType::Put as u8 => {
                let key = deserialize_buf(&mut r)?;
                let value = deserialize_buf(&mut r)?;
                Ok(KvOp::Put { key, value })
            }
            x if x == OpType::Delete as u8 => {
                let key = deserialize_buf(&mut r)?;
                Ok(KvOp::Delete { key })
            }
            _ => Err(Error::Corrupted("invalid type".into())),
        }
    }
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
    type E = Error;

    fn serialize(&self) -> Result<Vec<u8>> {
        let mut cursor = Cursor::new(vec![]);
        self.op.serialize_into(&mut cursor).unwrap();
        Ok(cursor.into_inner())
    }

    fn deserialize(buf: &[u8]) -> Result<Self> {
        let mut cursor = Cursor::new(buf);
        KvOp::deserialize_from(&mut cursor).map(|op| KvFsmOp { op })
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
