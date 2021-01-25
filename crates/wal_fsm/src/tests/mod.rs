mod sample_fsm;

pub use super::*;
use crossbeam::channel::Receiver;
pub use sample_fsm::*;

use std::time::{Duration, Instant};

const ENABLE_CHECK_NON_BLOCKING: bool = false;
const NON_BLOCKING_THRESHOLD: Duration = Duration::from_millis(10);

pub fn check_non_blocking(f: impl FnOnce()) {
    let now = Instant::now();
    f();
    let time = Instant::now() - now;
    if ENABLE_CHECK_NON_BLOCKING {
        assert!(
            time < NON_BLOCKING_THRESHOLD,
            "check_non_blocking failed ({}ms)",
            time.as_millis()
        );
    }
}

pub fn check_blocking(f: impl FnOnce(), dur: Duration) {
    let now = Instant::now();
    f();
    let time = Instant::now() - now;
    assert!(
        time >= dur,
        "check_blocking failed ({}ms)",
        time.as_millis()
    );
}

pub struct LogSyncWait {
    cur_lsn: Lsn,
    recv: Receiver<Lsn>,
}

impl LogSyncWait {
    pub fn create() -> (Self, Box<dyn Send + Sync + 'static + Fn(Lsn)>) {
        let (send, recv) = crossbeam::channel::bounded(0);

        let sink = Box::new(move |lsn| {
            send.send(lsn).unwrap();
        });

        (Self { cur_lsn: 0, recv }, sink)
    }

    pub fn wait(&mut self, lsn: Lsn) {
        while lsn > self.cur_lsn {
            self.cur_lsn = self.recv.recv().unwrap();
        }
    }
}
