use crate::{Lsn, Result};

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct LogRecord {
    pub op: Vec<u8>,
    pub lsn: Lsn,
}

pub(crate) trait LogRead: Send + Sync {
    fn read(&mut self, start: Lsn) -> Result<Box<dyn Iterator<Item = LogRecord>>>;
}

#[derive(Default)]
pub(crate) struct LogWriteOptions {
    pub force_sync: bool,
}

pub(crate) trait LogWrite: Send + Sync {
    fn init(&mut self, start_lsn: Lsn, sink: Box<dyn Send + Sync + Fn(Lsn)>) -> Result<()>;
    fn fire_write(&mut self, rec: LogRecord, options: &LogWriteOptions) -> Result<()>; // NOTE: this is fire and forgot
}

pub(crate) trait LogDiscard: Send + Sync {
    // Discard logs with lower Lsn.
    fn fire_discard(&mut self, lsn: Lsn) -> Result<()>;
}

pub struct LogCtx {
    pub(crate) read: Box<dyn LogRead>,
    pub(crate) write: Box<dyn LogWrite>,
    pub(crate) discard: Box<dyn LogDiscard>,
}
