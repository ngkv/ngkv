use std::{
    collections::VecDeque,
    marker::PhantomData,
    sync::{Arc, Mutex},
    todo,
};

use crate::LSN;
use anyhow::Result;

pub trait LogRead<Op>: Send + Sync {
    fn read(&mut self, start: LSN) -> Result<Box<dyn Iterator<Item = (Op, LSN)>>>;
}

pub trait LogWrite<Op>: Send + Sync {
    fn set_sync_sink(&mut self, sink: Box<dyn FnMut(LSN)>);
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

pub struct MemLogStorage<Op>(Arc<Mutex<MemLogInner<Op>>>);

impl<Op> MemLogStorage<Op> {
    pub fn new() -> Self {
        todo!()
    }
}

#[derive(Clone)]
struct MemLogRecord<Op> {
    lsn: LSN,
    op: Op,
}

struct MemLogInner<Op> {
    records: VecDeque<MemLogRecord<Op>>,
}

struct MemLogImpl<Op>(Arc<Mutex<MemLogInner<Op>>>);

impl<Op> LogRead<Op> for MemLogImpl<Op>
where
    Op: Send + Sync + Clone + 'static,
{
    fn read(&mut self, start: LSN) -> Result<Box<dyn Iterator<Item = (Op, LSN)>>> {
        let inner = self.0.lock().unwrap();
        let records = inner
            .records
            .iter()
            .filter(|rec| rec.lsn >= start)
            .cloned()
            .collect::<Vec<_>>();

        Ok(Box::new(records.into_iter().map(|rec| (rec.op, rec.lsn))))
    }
}

impl<Op> LogCtx<Op>
where
    Op: Send + Sync + Clone + 'static,
{
    pub fn memory(mem: &mut MemLogStorage<Op>) -> Self {
        todo!()
    }
}
