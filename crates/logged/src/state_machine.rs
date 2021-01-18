use crate::{LSN, ReportSink};

pub struct Init {
    pub next_lsn: LSN,
}

pub trait StateMachine: Send + Sync + 'static {
    type Op: 'static;
    fn init(&self, sink: Box<dyn ReportSink>) -> Init;
    fn apply(&self, op: Self::Op, lsn: LSN);
}

