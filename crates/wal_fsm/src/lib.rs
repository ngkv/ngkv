mod log_ctx;
mod log_file;
mod log_mem;
mod state_machine;
#[cfg(test)]
mod tests;

use never::Never;
use thiserror::Error;

// crate
pub use crate::{log_ctx::*, log_file::*, log_mem::*, state_machine::*};

// std
use std::{
    collections::VecDeque,
    error::Error as StdError,
    fmt::Debug,
    io,
    ops::Deref,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex, Weak,
    },
};

pub type Result<T, E = Error<Never>> = std::result::Result<T, E>;

#[derive(Error, Debug)]
pub enum Error<EFsm: StdError + 'static> {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("reportable bug: {0}")]
    ReportableBug(String),
    #[error("data corrupted: {0}")]
    Corrupted(String),
    #[error(transparent)]
    Fsm(Box<EFsm>),
}

impl<EFsm: StdError + 'static> From<&str> for Error<EFsm> {
    fn from(s: &str) -> Self {
        Self::ReportableBug(s.to_string())
    }
}

trait ResultNeverExt<T> {
    fn map_err_never<EFsm: StdError>(self) -> Result<T, Error<EFsm>>;
}

impl<T> ResultNeverExt<T> for Result<T, Error<Never>> {
    fn map_err_never<EFsm: StdError>(self) -> Result<T, Error<EFsm>> {
        self.map_err(|err| match err {
            Error::Io(e) => Error::Io(e),
            Error::ReportableBug(e) => Error::ReportableBug(e),
            Error::Corrupted(e) => Error::Corrupted(e),
            Error::Fsm(_) => unreachable!(),
        })
    }
}

// Log Sequence Number
// NOTE: LSN 1 represents the first log
pub type Lsn = u64;

pub trait ReportSink: Send + Sync {
    fn report_checkpoint_lsn(&self, lsn: Lsn);
}

struct PendingApply {
    lsn: Lsn,
    done: AtomicBool,
    cond: Condvar,
}

struct LoggedState {
    next_lsn: Lsn,
    log_write: Box<dyn LogWrite>,
    log_discard: Box<dyn LogDiscard>,
    pending_applies: VecDeque<Arc<PendingApply>>,
}

struct LoggedInner<S: Fsm> {
    state: Mutex<LoggedState>,
    sm: S,
}

pub struct WalFsm<S: Fsm>(Arc<LoggedInner<S>>);

struct ReportSinkImpl<S: Fsm> {
    logged: Weak<LoggedInner<S>>,
}

impl<S: Fsm> ReportSink for ReportSinkImpl<S> {
    fn report_checkpoint_lsn(&self, lsn: Lsn) {
        // update (highest included) snapshot lsn
        // which could be safely discarded
        let this = Weak::upgrade(&self.logged).expect("already destroyed");
        let mut state = this.state.lock().unwrap();

        // TODO handle discard error
        state
            .log_discard
            .fire_discard(lsn)
            .expect("error handling not supported");
    }
}

pub struct ApplyOptions {
    pub is_sync: bool,
}

impl<S: Fsm> Deref for WalFsm<S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        &self.0.sm
    }
}

impl<S: Fsm> LoggedInner<S> {
    fn finalize_pending(&self, lsn: Lsn) {
        let mut state = self.state.lock().unwrap();
        while let Some(p) = state.pending_applies.front() {
            if p.lsn > lsn {
                break;
            }

            p.done.store(true, Ordering::Relaxed);
            p.cond.notify_one();
            state.pending_applies.pop_front();
        }
    }
}

impl<S: Fsm> WalFsm<S> {
    pub fn new(sm: S, log_ctx: LogCtx) -> Result<Self, Error<S::E>> {
        let this = Arc::new(LoggedInner {
            sm,
            state: Mutex::new(LoggedState {
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
        let Init { next_lsn } = this.sm.init(sink).map_err(|e| Error::Fsm(Box::new(e)))?;
        assert!(next_lsn >= 1);

        {
            // set next_lsn
            let mut state = this.state.lock().unwrap();
            state.next_lsn = next_lsn;

            // replay log
            let mut log_read = log_ctx.read;
            for rec in log_read.read(next_lsn).map_err_never()? {
                assert_eq!(rec.lsn, state.next_lsn);
                this.sm
                    .apply(
                        S::Op::deserialize(&rec.op).expect("deserialize failed"),
                        state.next_lsn,
                    )
                    .map_err(|e| Error::Fsm(Box::new(e)))?;
                state.next_lsn += 1;
            }

            // set sink for log writer sync
            let next_lsn = state.next_lsn;
            state
                .log_write
                .init(
                    next_lsn,
                    Box::new(move |lsn| {
                        let this = this_weak.upgrade().expect("already destroyed");
                        this.finalize_pending(lsn);
                    }),
                )
                .map_err_never()?;
        }

        Ok(WalFsm(this))
    }

    pub fn apply(&self, op: S::Op, options: ApplyOptions) -> Result<(), Error<S::E>> {
        let op_buf = op.serialize().expect("error during serialization");

        let mut state = self.0.state.lock().unwrap();
        let lsn = state.next_lsn;
        state.next_lsn += 1;

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
            state.pending_applies.push_back(pending);
        }

        // TODO handle log write error
        state
            .log_write
            .fire_write(
                LogRecord { op: op_buf, lsn },
                &LogWriteOptions {
                    force_sync: options.is_sync,
                },
            )
            .expect("error handling not supported");

        if options.is_sync {
            // cur_pending must be Some
            let pending = cur_pending.unwrap();
            while !pending.done.load(Ordering::Relaxed) {
                state = pending.cond.wait(state).unwrap();
            }
        }

        self.0.sm.apply(op, lsn).map_err(|e| Error::Fsm(Box::new(e)))
    }
}
