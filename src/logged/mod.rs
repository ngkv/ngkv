#[cfg(test)]
mod test;

use std::{
    marker::PhantomData,
    sync::{Arc, Mutex, Weak},
};

// Log Sequence Number
// NOTE: LSN 1 represents the first log
pub type LSN = u64;

pub struct Init {
    pub next_lsn: LSN,
}

pub trait StateMachine<Op: 'static>: Send + Sync + 'static {
    fn init(&self, sink: Box<dyn ReportSink>) -> Init;
    fn apply(&self, op: Op, lsn: LSN);
}

pub trait ReportSink: Send + Sync {
    fn report_snapshot_lsn(&self, lsn: LSN);
}

struct LoggedImpl {
    next_lsn: LSN,
}

pub struct Logged<S: StateMachine<Op>, Op: 'static> {
    imp: Mutex<LoggedImpl>,
    sm: S,
    _op: PhantomData<fn(Op)>,
}

struct ReportSinkImpl<S: StateMachine<Op>, Op: 'static> {
    logged: Weak<Logged<S, Op>>,
}

impl<S: StateMachine<Op>, Op> Logged<S, Op> {
    pub fn new(sm: S) -> Arc<Self> {
        let logged = Arc::new(Self {
            sm,
            imp: Mutex::new(LoggedImpl { next_lsn: 1 }),
            _op: PhantomData::default(),
        });

        let sink = Box::new(ReportSinkImpl {
            logged: Arc::downgrade(&logged),
        });
        let init = logged.sm.init(sink);
        logged.imp.lock().unwrap().next_lsn = init.next_lsn;

        logged
    }
}

impl<S: StateMachine<Op>, Op> ReportSinkImpl<S, Op> {
    fn get_logged(&self) -> Arc<Logged<S, Op>> {
        Weak::upgrade(&self.logged).expect("receiver already dropped")
    }
}

impl<S: StateMachine<Op>, Op> ReportSink for ReportSinkImpl<S, Op> {
    fn report_snapshot_lsn(&self, lsn: LSN) {
        // TODO: update snapshot lsn
        // TODO: info log
        println!("report_snapshot_lsn: {}", lsn);
    }
}

pub enum ApplyOptions {
    Async = 0x1,
}