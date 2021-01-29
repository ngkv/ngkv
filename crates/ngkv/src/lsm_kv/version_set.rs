use crate::Result;
use bincode::{options, Options};
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use std::{path::Path, todo};
use wal_fsm::{Fsm, FsmOp, Lsn};

#[derive(Serialize, Deserialize, Clone)]
struct SstMeta {
    id: u32,
    level: u32,
    lsn_lo: Lsn,
    lsn_hi: Lsn,
    key_lo: Vec<u8>,
    key_hi: Vec<u8>,
}

#[derive(Serialize, Deserialize, Clone)]
struct VersionOp {
    sst_add: Vec<SstMeta>,
    sst_del: Vec<u32>,
}

fn bincode_options() -> impl bincode::Options {
    bincode::DefaultOptions::new()
        .with_little_endian()
        .with_varint_encoding()
}

impl FsmOp for VersionOp {
    fn serialize(&self) -> wal_fsm::Result<Vec<u8>> {
        Ok(bincode_options().serialize(&self).unwrap())
    }

    fn deserialize(buf: &[u8]) -> wal_fsm::Result<Self> {
        bincode_options()
            .deserialize(buf)
            .map_err(|_| wal_fsm::Error::Corrupted("deserialization failed".into()))
    }
}

struct Version {
    
}

struct VersionFsm {
    sink: OnceCell<Box<dyn wal_fsm::ReportSink>>,
}

impl VersionFsm {
    pub fn new(dir: &Path) -> Self {
        todo!()
    }

    /// Garbage collect unused files.
    fn gc(&self) -> Result<()> {
        todo!()
    }
}

impl Fsm for VersionFsm {
    type Op = VersionOp;

    fn init(&self, sink: Box<dyn wal_fsm::ReportSink>) -> wal_fsm::Init {
        todo!()
    }

    fn apply(&self, op: Self::Op, lsn: wal_fsm::Lsn) {
        todo!()
    }
}
