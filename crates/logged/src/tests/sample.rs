use super::*;

use crossbeam::channel::{Receiver, Sender};
use mockall::*;
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};

use std::{
    sync::Arc,
    sync::{Mutex, MutexGuard},
    thread::{sleep, JoinHandle},
    time::Duration,
};

const STORAGE_DELAY: Duration = Duration::from_millis(20);
const PERSISTER_TEST_WAIT: Duration = Duration::from_millis(50);

#[derive(Serialize, Deserialize, Clone)]
pub enum SampleOp {
    AddOne, // add one, return result
}

pub struct SampleStorage {
    data: Mutex<(LSN, i32)>,
}

impl SampleStorage {
    fn new(lsn: LSN, num: i32) -> Self {
        SampleStorage {
            data: (lsn, num).into(),
        }
    }

    fn load(&self) -> (LSN, i32) {
        sleep(STORAGE_DELAY);
        *self.data.lock().unwrap()
    }

    fn save(&self, lsn: LSN, num: i32) {
        sleep(STORAGE_DELAY);
        let mut lck = self.data.lock().unwrap();
        let (prev_lsn, _) = *lck;
        assert!(lsn >= prev_lsn);
        *lck = (lsn, num);
    }
}

#[derive(Debug)]
struct SampleStateMachineState {
    num: i32,
    lsn: LSN,
    persister_thread: Option<JoinHandle<()>>,
    persister_do_send: Sender<()>,
    persister_kill_send: Sender<()>,
}

struct SampleStateMachineInner {
    state: OnceCell<Mutex<SampleStateMachineState>>,
    storage: Arc<SampleStorage>,
    sink: OnceCell<Box<dyn ReportSink>>,
}

impl SampleStateMachineInner {
    fn get_state<'a>(&'a self) -> MutexGuard<'a, SampleStateMachineState> {
        self.state.get().expect("not initialized").lock().unwrap()
    }

    fn try_get_state<'a>(&'a self) -> Option<MutexGuard<'a, SampleStateMachineState>> {
        Some(self.state.get()?.lock().unwrap())
    }

    fn do_work_persister(self: Arc<Self>, do_recv: Receiver<()>, kill_recv: Receiver<()>) {
        let mut sel = crossbeam::channel::Select::new();
        let op_do = sel.recv(&do_recv);
        let op_kill = sel.recv(&kill_recv);

        // FIXME: use select! instead
        // currently some problem with macro expansion
        loop {
            let op = sel.select();
            match op.index() {
                i if i == op_do => {
                    op.recv(&do_recv).unwrap();
                    let state = self.get_state();
                    let (lsn, num) = (state.lsn, state.num);
                    drop(state);

                    self.storage.save(lsn, num);
                    self.sink
                        .get()
                        .expect("should be initialized")
                        .report_snapshot_lsn(lsn);
                }
                i if i == op_kill => {
                    op.recv(&kill_recv).unwrap();
                    break;
                }
                _ => {}
            }
        }
    }
}

pub struct SampleStateMachine(Arc<SampleStateMachineInner>);

impl SampleStateMachine {
    pub fn new(storage: Arc<SampleStorage>) -> Self {
        Self(Arc::new(SampleStateMachineInner {
            state: OnceCell::new().into(),
            storage,
            sink: OnceCell::new(),
        }))
    }

    pub fn fire_persist(&self) {
        self.0
            .get_state()
            .persister_do_send
            .try_send(())
            .expect("fire failed");
    }

    pub fn get_num(&self) -> i32 {
        self.0.get_state().num
    }
}

impl Drop for SampleStateMachine {
    fn drop(&mut self) {
        let _thread;
        if let Some(mut state) = self.0.try_get_state() {
            state
                .persister_kill_send
                .send(())
                .expect("kill persister failed");
            _thread = state.persister_thread.take().unwrap();
        }
        // JoinHandle _thread drops here, without state mutex held.
    }
}

impl StateMachine for SampleStateMachine {
    type Op = SampleOp;

    fn init(&self, sink: Box<dyn ReportSink>) -> Init {
        self.0
            .sink
            .set(sink)
            .map_err(|_| ())
            .expect("init called twice");

        // read from persist storage
        let (lsn, num) = self.0.storage.load();

        // channels for persister thread
        let (kill_send, kill_recv) = crossbeam::channel::bounded(1);
        let (do_send, do_recv) = crossbeam::channel::bounded(1);

        self.0
            .state
            .set(
                SampleStateMachineState {
                    lsn,
                    num,
                    persister_kill_send: kill_send,
                    persister_do_send: do_send,
                    persister_thread: Some({
                        let this = self.0.clone();
                        std::thread::spawn(|| this.do_work_persister(do_recv, kill_recv))
                    }),
                }
                .into(),
            )
            .expect("init called twice");

        Init { next_lsn: lsn + 1 }
    }

    fn apply(&self, op: SampleOp, lsn: LSN) {
        let mut state = self.0.get_state();

        assert_eq!(state.lsn + 1, lsn);
        state.lsn = lsn;

        match op {
            SampleOp::AddOne => {
                state.num += 1;
            }
        }
    }
}

mock! {
    pub SampleReportSink {}
    impl ReportSink for SampleReportSink {
        fn report_snapshot_lsn(&self, _lsn: LSN);
    }
}

#[test]
fn sample_sm_normal() {
    // LSN: 3, Data: 1
    let storage = Arc::new(SampleStorage::new(3, 1));
    let sm = SampleStateMachine::new(storage.clone());

    let mut mock = MockSampleReportSink::new();
    let mut seq = Sequence::new();
    mock.expect_report_snapshot_lsn()
        .with(predicate::eq(5))
        .times(1)
        .returning(|_| ())
        .in_sequence(&mut seq);
    mock.expect_report_snapshot_lsn()
        .with(predicate::eq(6))
        .times(1)
        .returning(|_| ())
        .in_sequence(&mut seq);

    let Init { next_lsn } = sm.init(Box::new(mock));
    assert_eq!(next_lsn, 4);

    {
        sm.apply(SampleOp::AddOne, 4);
        assert_eq!(sm.get_num(), 2);
    }

    {
        sm.apply(SampleOp::AddOne, 5);
        assert_eq!(sm.get_num(), 3);
    }

    sm.fire_persist();
    sleep(PERSISTER_TEST_WAIT); // wait persister to finish

    {
        sm.apply(SampleOp::AddOne, 6);
        assert_eq!(sm.get_num(), 4);
    }

    {
        let (lsn, num) = storage.load();
        assert_eq!(lsn, 5);
        assert_eq!(num, 3);
    }

    sm.fire_persist();
    sleep(PERSISTER_TEST_WAIT); // wait persister to finish

    {
        let (lsn, num) = storage.load();
        assert_eq!(lsn, 6);
        assert_eq!(num, 4);
    }
}

#[test]
fn sample_sm_not_durable() {
    let storage = Arc::new(SampleStorage::new(3, 1));
    let sm = SampleStateMachine::new(storage.clone());

    let mut mock = MockSampleReportSink::new();
    mock.expect_report_snapshot_lsn()
        .with(predicate::eq(4))
        .times(1)
        .returning(|_| ());

    let Init { next_lsn } = sm.init(Box::new(mock));
    assert_eq!(next_lsn, 4);

    {
        sm.apply(SampleOp::AddOne, 4);
        assert_eq!(sm.get_num(), 2);
    }

    sm.fire_persist();

    {
        let (lsn, num) = storage.load();
        assert_eq!(lsn, 3);
        assert_eq!(num, 1);
    }

    sleep(PERSISTER_TEST_WAIT); // wait persister to finish

    {
        let (lsn, num) = storage.load();
        assert_eq!(lsn, 4);
        assert_eq!(num, 2);
    }
}

#[test]
fn logged_sample_sm_normal() -> Result<()> {
    let storage = Arc::new(SampleStorage::new(3, 1));

    let sm = SampleStateMachine::new(storage.clone());
    let mut mem_log = MemLogStorage::new(Duration::from_secs(0));
    let logged = Logged::new(sm, LogCtx::memory(&mut mem_log))?;

    logged.apply(SampleOp::AddOne, ApplyOptions { is_sync: true });
    assert_eq!(logged.get_num(), 2);

    logged.apply(SampleOp::AddOne, ApplyOptions { is_sync: true });
    assert_eq!(logged.get_num(), 3);

    logged.apply(SampleOp::AddOne, ApplyOptions { is_sync: false });
    logged.apply(SampleOp::AddOne, ApplyOptions { is_sync: false });

    logged.apply(SampleOp::AddOne, ApplyOptions { is_sync: true });
    assert_eq!(logged.get_num(), 6);

    logged.fire_persist();
    sleep(PERSISTER_TEST_WAIT);

    {
        let (lsn, num) = storage.load();
        assert_eq!(lsn, 8);
        assert_eq!(num, 6);
    }

    Ok(())
}

#[test]
fn logged_sample_sm_sync_durable() -> Result<()> {
    let storage = Arc::new(SampleStorage::new(3, 1));
    let mut mem_log = MemLogStorage::new(Duration::from_millis(50));

    {
        let sm = SampleStateMachine::new(storage.clone());
        let logged = Logged::new(sm, LogCtx::memory(&mut mem_log))?;

        check_blocking(
            || logged.apply(SampleOp::AddOne, ApplyOptions { is_sync: true }),
            Duration::from_millis(50),
        );
        assert_eq!(logged.get_num(), 2);

        check_blocking(
            || logged.apply(SampleOp::AddOne, ApplyOptions { is_sync: true }),
            Duration::from_millis(50),
        );
        assert_eq!(logged.get_num(), 3);

        check_non_blocking(|| drop(logged));
    }

    {
        let sm = SampleStateMachine::new(storage.clone());
        let logged = Logged::new(sm, LogCtx::memory(&mut mem_log))?;

        // check not persisted by sm itself
        let (_, num) = storage.load();
        assert_ne!(num, 3);

        // check persisted by log
        assert_eq!(logged.get_num(), 3);
    }

    Ok(())
}

#[test]
fn logged_sample_sm_async_lost() -> Result<()> {
    let storage = Arc::new(SampleStorage::new(3, 1));
    let mut mem_log = MemLogStorage::new(Duration::from_millis(50));

    {
        let sm = SampleStateMachine::new(storage.clone());
        let logged = Logged::new(sm, LogCtx::memory(&mut mem_log))?;

        check_non_blocking(|| logged.apply(SampleOp::AddOne, ApplyOptions { is_sync: false }));
        assert_eq!(logged.get_num(), 2);

        check_non_blocking(|| logged.apply(SampleOp::AddOne, ApplyOptions { is_sync: false }));
        assert_eq!(logged.get_num(), 3);

        check_non_blocking(|| drop(logged));
    }

    sleep(Duration::from_millis(100));

    {
        let sm = SampleStateMachine::new(storage.clone());
        let logged = Logged::new(sm, LogCtx::memory(&mut mem_log))?;

        // check not persisted by sm itself
        let (_, num) = storage.load();
        assert_ne!(num, 3);

        // check not persisted by log
        assert_ne!(logged.get_num(), 3);
    }

    Ok(())
}
