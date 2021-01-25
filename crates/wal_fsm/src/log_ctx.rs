use crate::Lsn;
use anyhow::Result;

pub trait LogRead<Op>: Send + Sync {
    fn read(&mut self, start: Lsn) -> Result<Box<dyn Iterator<Item = (Op, Lsn)>>>;
}

#[derive(Default)]
pub struct LogWriteOptions {
    pub force_sync: bool,
}

pub trait LogWrite<Op>: Send + Sync {
    fn init(&mut self, start_lsn: Lsn, sink: Box<dyn Send + Sync + Fn(Lsn)>) -> Result<()>;
    fn fire_write(&mut self, op: &Op, lsn: Lsn, options: &LogWriteOptions) -> Result<()>; // NOTE: this is fire and forgot
}

pub trait LogDiscard: Send + Sync {
    fn fire_discard(&mut self, lsn: Lsn) -> Result<()>;
}

pub struct LogCtx<Op> {
    pub(crate) read: Box<dyn LogRead<Op>>,
    pub(crate) write: Box<dyn LogWrite<Op>>,
    pub(crate) discard: Box<dyn LogDiscard>,
}
