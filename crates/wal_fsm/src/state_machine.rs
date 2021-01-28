use crate::{FsmOp, Lsn, ReportSink};

pub struct Init {
    pub next_lsn: Lsn,
}

pub trait Fsm: Send + Sync + 'static {
    type Op: FsmOp;
    fn init(&self, sink: Box<dyn ReportSink>) -> Init;
    fn apply(&self, op: Self::Op, lsn: Lsn);
}

