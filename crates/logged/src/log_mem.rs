use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex,
    },
    thread::{spawn, JoinHandle},
    time::{Duration, Instant},
};

use crate::{LogCtx, LogDiscard, LogRead, LogWrite, LSN};
use anyhow::Result;
use once_cell::sync::OnceCell;

pub struct MemLogStorage<Op>(Arc<MemLogInner<Op>>);

impl<Op> MemLogStorage<Op> {
    pub fn new(sync_delay: Duration) -> Self {
        Self(Arc::new(MemLogInner {
            sync_delay,
            sync_killed: AtomicBool::new(false),
            sync_cond: Condvar::new(),
            sync_sink: OnceCell::new(),
            state: Mutex::new(MemLogState {
                records: VecDeque::new(),
                pendings: VecDeque::new(),
                cur_lsn: 0,
                sync_thread: None,
            }),
        }))
    }

    pub fn len(&self) -> usize {
        self.0.state.lock().unwrap().records.len()
    }
}

#[derive(Clone)]
struct MemLogRecord<Op> {
    lsn: LSN,
    op: Op,
}

struct PendingLog<Op> {
    op: Op,
    lsn: LSN,
    sync_time: Instant,
}

struct MemLogState<Op> {
    records: VecDeque<MemLogRecord<Op>>,
    pendings: VecDeque<PendingLog<Op>>,
    cur_lsn: LSN,
    sync_thread: Option<JoinHandle<()>>,
}

struct SyncState {
    sync_thread: Option<JoinHandle<()>>,
    sync_killed: AtomicBool,
    sync_cond: Condvar,
    sync_delay: Duration,
    sync_sink: OnceCell<Box<dyn Send + Sync + Fn(LSN)>>,
}

struct MemLogInner<Op> {
    state: Mutex<MemLogState<Op>>,
    sync_killed: AtomicBool,
    sync_cond: Condvar,
    sync_delay: Duration,
    sync_sink: OnceCell<Box<dyn Send + Sync + Fn(LSN)>>,
}

struct MemLogImpl<Op>(Arc<MemLogInner<Op>>);

impl<Op> MemLogImpl<Op> {
    fn spawn_sync_thread(&self)
    where
        Op: Send + Sync + 'static,
    {
        let mut state = self.0.state.lock().unwrap();
        assert!(state.sync_thread.is_none());

        let this = self.0.clone();
        state.sync_thread = Some(spawn(move || {
            loop {
                let mut state = this.state.lock().unwrap();

                // wait till one of following occurs:
                // 1. killed
                // 2. sync time of *first* record in the pending queue has been reached
                while !this.sync_killed.load(Ordering::Relaxed) {
                    if let Some(p) = state.pendings.front() {
                        let now = Instant::now();
                        if p.sync_time <= now {
                            break;
                        } else {
                            let sleep = p.sync_time - now;
                            let (s, _) = this.sync_cond.wait_timeout(state, sleep).unwrap();
                            state = s;
                        }
                    } else {
                        state = this.sync_cond.wait(state).unwrap();
                    }
                }

                if this.sync_killed.load(Ordering::Relaxed) {
                    break;
                }

                // find the highest possible sync lsn
                let mut sync_lsn = None;
                while let Some(p) = state.pendings.front() {
                    if p.sync_time <= Instant::now() {
                        let p = state.pendings.pop_front().unwrap();
                        sync_lsn = Some(p.lsn);
                        state.records.push_back(MemLogRecord {
                            lsn: p.lsn,
                            op: p.op,
                        });
                    } else {
                        break;
                    }
                }

                // garunteed by cond var
                let sync_lsn = sync_lsn.expect("sync lsn not found");

                // drop lock to invoke sync sink
                // prevent deadlock
                drop(state);
                let sync_sink = this.sync_sink.get().expect("not init");
                sync_sink(sync_lsn);
            }
        }));
    }

    fn kill_sync_thread(&self) {
        self.0.sync_killed.store(true, Ordering::Relaxed);
        self.0.sync_cond.notify_one();

        let _thread = self.0.state.lock().unwrap().sync_thread.take();
    }
}

impl<Op> Drop for MemLogImpl<Op> {
    fn drop(&mut self) {
        self.kill_sync_thread();
    }
}

impl<Op> LogRead<Op> for MemLogImpl<Op>
where
    Op: Send + Sync + Clone + 'static,
{
    fn read(&mut self, start: LSN) -> Result<Box<dyn Iterator<Item = (Op, LSN)>>> {
        let state = self.0.state.lock().unwrap();
        let records = state
            .records
            .iter()
            .filter(|rec| rec.lsn >= start)
            .cloned()
            .collect::<Vec<_>>();

        Ok(Box::new(records.into_iter().map(|rec| (rec.op, rec.lsn))))
    }
}

impl<Op> LogWrite<Op> for MemLogImpl<Op>
where
    Op: Send + Sync + Clone + 'static,
{
    fn init(&mut self, sink: Box<dyn Send + Sync + Fn(LSN)>) {
        self.0
            .sync_sink
            .set(sink)
            .map_err(|_| ())
            .expect("already init");

        self.spawn_sync_thread();
    }

    fn fire_write(&mut self, op: &Op, lsn: LSN) {
        let mut state = self.0.state.lock().unwrap();

        assert!(state.cur_lsn < lsn);
        state.cur_lsn = lsn;

        state.pendings.push_back(PendingLog {
            lsn,
            op: op.clone(),
            sync_time: Instant::now() + self.0.sync_delay,
        });
        self.0.sync_cond.notify_one();
    }
}

impl<Op> LogDiscard for MemLogImpl<Op>
where
    Op: Send + Sync + Clone + 'static,
{
    fn discard(&mut self, lsn: LSN) {
        let mut state = self.0.state.lock().unwrap();
        while let Some(rec) = state.records.front() {
            if rec.lsn <= lsn {
                state.records.pop_front();
            } else {
                break;
            }
        }
    }
}

impl<Op> LogCtx<Op>
where
    Op: Send + Sync + Clone + 'static,
{
    pub fn memory(mem: &mut MemLogStorage<Op>) -> Self {
        let mk_boxed = || Box::new(MemLogImpl::<Op>(mem.0.clone()));
        Self {
            read: mk_boxed(),
            write: mk_boxed(),
            discard: mk_boxed(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{thread::sleep, time::Duration, vec};

    use crate::{LogCtx, MemLogStorage, LSN};

    #[derive(Clone, Eq, PartialEq, Debug)]
    struct TestOp {
        no: LSN, // equal to LSN
    }

    #[test]
    fn mem_log_normal() {
        let mut stor = MemLogStorage::new(Duration::from_millis(50));

        let mut ctx = LogCtx::<TestOp>::memory(&mut stor);
        let (send, recv) = crossbeam::channel::bounded(0);

        ctx.write.init(Box::new(move |lsn| {
            send.send(lsn).unwrap();
        }));

        let mut cur_lsn = 0;
        let cur_lsn_ref = &mut cur_lsn;
        let mut expect_lsn_sync = move |lsn: LSN| {
            while lsn > *cur_lsn_ref {
                *cur_lsn_ref = recv.recv().unwrap();
            }
        };

        ctx.write.fire_write(&TestOp { no: 1 }, 1);
        ctx.write.fire_write(&TestOp { no: 2 }, 2);

        expect_lsn_sync(2);

        {
            let read_res = ctx.read.read(0).unwrap().collect::<Vec<_>>();
            assert_eq!(read_res, vec![(TestOp { no: 1 }, 1), (TestOp { no: 2 }, 2)]);
        }

        {
            let read_res = ctx.read.read(2).unwrap().collect::<Vec<_>>();
            assert_eq!(read_res, vec![(TestOp { no: 2 }, 2)]);
        }

        ctx.discard.discard(1);

        {
            let read_res = ctx.read.read(0).unwrap().collect::<Vec<_>>();
            assert_eq!(read_res, vec![(TestOp { no: 2 }, 2)]);
        }
    }

    #[test]
    fn mem_log_drop_no_sync() {
        let mut stor = MemLogStorage::new(Duration::from_millis(50));

        {
            let mut ctx = LogCtx::<TestOp>::memory(&mut stor);
            ctx.write.init(Box::new(|_| {}));
            ctx.write.fire_write(&TestOp { no: 1 }, 1);
            ctx.write.fire_write(&TestOp { no: 2 }, 2);
        }

        // wait at least sync_delay
        sleep(Duration::from_millis(100));

        {
            let mut ctx = LogCtx::<TestOp>::memory(&mut stor);
            let read_res = ctx.read.read(0).unwrap().collect::<Vec<_>>();
            assert_eq!(read_res, vec![]);
        }
    }
}
