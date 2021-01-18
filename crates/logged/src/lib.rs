mod log;
mod state_machine;
#[cfg(test)]
mod tests;

use anyhow::Result;

// crate
use crate::{log::*, state_machine::*};

// std
use std::{
    collections::VecDeque,
    ops::Deref,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex, Weak,
    },
};

// Log Sequence Number
// NOTE: LSN 1 represents the first log
pub type LSN = u64;

pub trait ReportSink: Send + Sync {
    fn report_snapshot_lsn(&self, lsn: LSN);
}

struct PendingApply {
    lsn: LSN,
    done: AtomicBool,
    cond: Condvar,
}

struct LoggedImpl<Op> {
    next_lsn: LSN,
    log_write: Box<dyn LogWrite<Op>>,
    log_discard: Box<dyn LogDiscard>,
    pending_applies: VecDeque<Arc<PendingApply>>,
}

pub struct Logged<S: StateMachine> {
    imp: Mutex<LoggedImpl<S::Op>>,
    sm: S,
}

struct ReportSinkImpl<S: StateMachine> {
    logged: Weak<Logged<S>>,
}

impl<S: StateMachine> ReportSink for ReportSinkImpl<S> {
    fn report_snapshot_lsn(&self, lsn: LSN) {
        // update (highest included) snapshot lsn
        // which could be safely discarded
        let this = Weak::upgrade(&self.logged).expect("already destroyed");
        let mut lck = this.imp.lock().unwrap();
        lck.log_discard.discard(lsn);
    }
}

pub struct ApplyOptions {
    pub is_sync: bool,
}

impl<S: StateMachine> Deref for Logged<S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        &self.sm
    }
}

impl<S: StateMachine> Logged<S> {
    pub fn new(sm: S, log_ctx: LogCtx<S::Op>) -> Result<Arc<Self>> {
        let this = Arc::new(Self {
            sm,
            imp: Mutex::new(LoggedImpl {
                next_lsn: 1,
                log_write: log_ctx.write,
                log_discard: log_ctx.discard,
                pending_applies: VecDeque::new(),
            }),
        });

        let this_weak = Arc::downgrade(&this);

        let sink = Box::new(ReportSinkImpl {
            logged: this_weak.clone(),
        });

        // init state machine (e.g. read from persistent store)
        let Init { next_lsn } = this.sm.init(sink);
        assert!(next_lsn >= 1);

        {
            // set next_lsn & replay log
            let mut lck = this.imp.lock().unwrap();
            lck.next_lsn = next_lsn;
            let mut log_read = log_ctx.read;
            for (op, lsn) in log_read.read(next_lsn)? {
                assert_eq!(lsn, lck.next_lsn);
                this.sm.apply(op, lck.next_lsn);
                lck.next_lsn += 1;
            }

            // set sink for log writer sync
            lck.log_write.set_sync_sink(Box::new(move |lsn| {
                let this = this_weak.upgrade().expect("already destroyed");
                this.finalize_pending(lsn);
            }));
        }

        Ok(this)
    }

    fn finalize_pending(&self, lsn: LSN) {
        let mut lck = self.imp.lock().unwrap();
        while let Some(p) = lck.pending_applies.front() {
            if p.lsn > lsn {
                break;
            }

            p.done.store(true, Ordering::Release);
            p.cond.notify_one();
            lck.pending_applies.pop_front();
        }
    }

    pub fn apply(&self, op: S::Op, options: ApplyOptions) {
        let mut lck = self.imp.lock().unwrap();
        let lsn = lck.next_lsn;
        lck.next_lsn += 1;

        // NOTE:
        // we must set pending apply before fire_write
        let mut cur_pending = None;
        if options.is_sync {
            let pending = Arc::new(PendingApply {
                done: AtomicBool::new(false),
                lsn,
                cond: Condvar::new(),
            });
            cur_pending = Some(pending.clone());
            lck.pending_applies.push_back(pending);
        }

        lck.log_write.fire_write(&op, lsn);

        if options.is_sync {
            // cur_pending must be Some
            let pending = cur_pending.unwrap();
            while pending.done.load(Ordering::Acquire) {
                lck = pending.cond.wait(lck).unwrap();
            }
        }

        self.sm.apply(op, lsn)
    }
}
