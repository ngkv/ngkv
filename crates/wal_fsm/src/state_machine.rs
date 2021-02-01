use std::error::Error as StdError;

use crate::{Lsn, ReportSink, Result};

pub struct Init {
    pub next_lsn: Lsn,
}

pub trait FsmOp: Send + Sync + Sized + 'static {
    type E: StdError;
    fn serialize(&self) -> Result<Vec<u8>, Self::E>;
    fn deserialize(buf: &[u8]) -> Result<Self, Self::E>;
}

pub trait Fsm: Send + Sync + 'static {
    type E: StdError;
    type Op: FsmOp<E=Self::E>;
    fn init(&self, sink: Box<dyn ReportSink>) -> Result<Init, Self::E>;
    fn apply(&self, op: Self::Op, lsn: Lsn) -> Result<(), Self::E>;
}
