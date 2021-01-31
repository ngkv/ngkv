use std::error::Error as StdError;

use crate::{FsmOp, Lsn, ReportSink, Result};

pub struct Init {
    pub next_lsn: Lsn,
}

pub trait Fsm: Send + Sync + 'static {
    type Op: FsmOp;
    type E: StdError;
    fn init(&self, sink: Box<dyn ReportSink>) -> Result<Init, Self::E>;
    fn apply(&self, op: Self::Op, lsn: Lsn) -> Result<(), Self::E>;
}
