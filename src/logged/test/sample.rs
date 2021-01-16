use super::*;

use crossbeam::channel::{Receiver, Sender};
use once_cell::sync::OnceCell;
use mockall::*;

use std::{
    sync::Arc,
    sync::{Mutex, MutexGuard},
    thread::{self, JoinHandle},
    time::Duration,
};


use std::cell::Cell;


const PERSISTER_INTERVAL: Duration = std::time::Duration::from_secs(1);

enum SampleOp {
    AddOne(Arc<Cell<i32>>), // add one, return result
}

struct SampleStorage {
    data: Mutex<(LSN, i32)>,
}

impl SampleStorage {
    fn new() -> Self {
        SampleStorage {
            data: (0, 0).into(),
        }
    }

    fn load(&self) -> (LSN, i32) {
        // TODO: some delay?
        *self.data.lock().unwrap()
    }

    fn save(&self, lsn: LSN, num: i32) {
        // TODO: some delay?
        let lck = self.data.lock().unwrap();
        let (prev_lsn, _) = *lck;
        assert!(lsn >= prev_lsn);
        *self.data.lock().unwrap() = (lsn, num);
    }
}

#[derive(Debug)]
struct SampleStateMachineInner {
    num: i32,
    lsn: LSN,
    persister_thread: JoinHandle<()>,
    persister_kill_send: Sender<()>,
}

struct SampleStateMachineImpl {
    inner: OnceCell<Mutex<SampleStateMachineInner>>,
    storage: Arc<SampleStorage>,
    sink: OnceCell<Box<dyn ReportSink>>,
}

impl SampleStateMachineImpl {
    fn get_inner<'a>(&'a self) -> MutexGuard<'a, SampleStateMachineInner> {
        self.inner.get().expect("not initialized").lock().unwrap()
    }

    fn do_work_persister(self: Arc<Self>, kill_recv: Receiver<()>) {
        while let Err(_) = kill_recv.recv_timeout(PERSISTER_INTERVAL) {
            let inner = self.get_inner();
            let (lsn, num) = (inner.lsn, inner.num);
            drop(inner);

            self.storage.save(lsn, num);
            self.sink
                .get()
                .expect("should be initialized")
                .report_snapshot_lsn(lsn);
        }
    }
}

struct SampleStateMachine(Arc<SampleStateMachineImpl>);

impl SampleStateMachine {
    fn new(storage: Arc<SampleStorage>) -> Self {
        Self(Arc::new(SampleStateMachineImpl {
            inner: OnceCell::new().into(),
            storage,
            sink: OnceCell::new(),
        }))
    }
}

impl Drop for SampleStateMachine {
    fn drop(&mut self) {
        self.0
            .get_inner()
            .persister_kill_send
            .send(())
            .expect("kill persister failed");
    }
}

impl StateMachine<SampleOp> for SampleStateMachine {
    fn init(&self, sink: Box<dyn ReportSink>) -> Init {
        self.0
            .sink
            .set(sink)
            .map_err(|_| ())
            .expect("init called twice");

        // read from persist storage
        let (lsn, num) = self.0.storage.load();

        let (send, recv) = crossbeam::channel::bounded(1);

        self.0
            .inner
            .set(
                SampleStateMachineInner {
                    lsn,
                    num,
                    persister_kill_send: send,
                    persister_thread: {
                        let this = self.0.clone();
                        thread::spawn(|| this.do_work_persister(recv))
                    },
                }
                .into(),
            )
            .expect("init called twice");

        Init { next_lsn: lsn + 1 }
    }

    fn apply(&self, op: SampleOp, _lsn: LSN) {
        let mut inner = self.0.get_inner();
        match op {
            SampleOp::AddOne(dest) => {
                inner.num += 1;
                dest.set(inner.num);
            }
        }
    }
}

struct SampleReportSink { }

#[automock]
impl ReportSink for SampleReportSink
{
    fn report_snapshot_lsn(&self, lsn: LSN) {
        todo!()
    }
}

#[test]
fn test_sample_simple() {
    let storage = Arc::new(SampleStorage::new());
    let sm = SampleStateMachine::new(storage);
}
