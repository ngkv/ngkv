use crate::LSN;
use anyhow::Result;

pub trait LogRead<Op>: Send + Sync {
    fn read(&mut self, start: LSN) -> Result<Box<dyn Iterator<Item = (Op, LSN)>>>;
}

pub trait LogWrite<Op>: Send + Sync {
    fn init(&mut self, sink: Box<dyn Send + Sync + Fn(LSN)>);
    fn fire_write(&mut self, op: &Op, lsn: LSN); // NOTE: this is fire and forgot
}

pub trait LogDiscard: Send + Sync {
    fn discard(&mut self, lsn: LSN);
}
pub struct LogCtx<Op> {
    pub(crate) read: Box<dyn LogRead<Op>>,
    pub(crate) write: Box<dyn LogWrite<Op>>,
    pub(crate) discard: Box<dyn LogDiscard>,
}
