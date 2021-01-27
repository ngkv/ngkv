use crate::{Lsn, Result};

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct LogRecord<Op> {
    pub op: Op,
    pub lsn: Lsn,
}

pub(crate) trait LogRead<Op>: Send + Sync {
    fn read(&mut self, start: Lsn) -> Result<Box<dyn Iterator<Item = LogRecord<Op>>>>;
}

#[derive(Default)]
pub(crate) struct LogWriteOptions {
    pub force_sync: bool,
}

pub(crate) trait LogWrite<Op>: Send + Sync {
    fn init(&mut self, start_lsn: Lsn, sink: Box<dyn Send + Sync + Fn(Lsn)>) -> Result<()>;
    fn fire_write(&mut self, rec: &LogRecord<Op>, options: &LogWriteOptions) -> Result<()>; // NOTE: this is fire and forgot
}

pub(crate) trait LogDiscard: Send + Sync {
    // Discard logs with lower Lsn.
    fn fire_discard(&mut self, lsn: Lsn) -> Result<()>;
}

pub struct LogCtx<Op> {
    pub(crate) read: Box<dyn LogRead<Op>>,
    pub(crate) write: Box<dyn LogWrite<Op>>,
    pub(crate) discard: Box<dyn LogDiscard>,
}
