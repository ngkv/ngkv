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
use smart_default::SmartDefault;

pub struct MemLogStorage<Op>(Arc<StorageShared<Op>>);

impl<Op> MemLogStorage<Op> {
    pub fn new(sync_delay: Duration) -> Self {
        Self(Arc::new(StorageShared {
            sync_delay,
            ..Default::default()
        }))
    }

    pub fn len(&self) -> usize {
        self.0.records.lock().unwrap().len()
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

#[derive(SmartDefault)]
struct StorageShared<Op> {
    records: Mutex<VecDeque<MemLogRecord<Op>>>,
    sync_delay: Duration,
}
#[derive(SmartDefault)]
struct SyncShared<Op> {
    pendings: Mutex<VecDeque<PendingLog<Op>>>,
    thread: Mutex<Option<JoinHandle<()>>>,
    sink: OnceCell<Box<dyn Send + Sync + Fn(LSN)>>,
    killed: AtomicBool,
    cond: Condvar,
}

struct ReadDiscardImpl<Op> {
    stor: Arc<StorageShared<Op>>,
}

impl<Op> ReadDiscardImpl<Op> {
    fn new(stor: Arc<StorageShared<Op>>) -> Self {
        Self { stor }
    }
}

// LOCK ORDER: sync, stor
struct WriteImpl<Op> {
    sync: Arc<SyncShared<Op>>,
    stor: Arc<StorageShared<Op>>,
}

impl<Op> Drop for WriteImpl<Op> {
    fn drop(&mut self) {
        self.sync.killed.store(true, Ordering::Relaxed);
        self.sync.cond.notify_one();

        // Join thread here. This ensures that sync sink would not be called
        // after drop.
        let _thread = self.sync.thread.lock().unwrap().take();
    }
}

impl<Op> WriteImpl<Op> {
    fn new(stor: Arc<StorageShared<Op>>) -> Self
    where
        Op: Send + Sync + 'static,
    {
        let sync = Arc::new(SyncShared::default());

        let thread = spawn({
            let sync = sync.clone();
            let stor = stor.clone();
            move || {
                loop {
                    let mut pendings = sync.pendings.lock().unwrap();

                    // Wait till one of following occurs:
                    // 1. Killed.
                    // 2. Sync time of *first* record in the pending queue has
                    //    been reached.
                    while !sync.killed.load(Ordering::Relaxed) {
                        if let Some(p) = pendings.front() {
                            let now = Instant::now();
                            if p.sync_time <= now {
                                break;
                            } else {
                                let sleep = p.sync_time - now;
                                let (s, _) = sync.cond.wait_timeout(pendings, sleep).unwrap();
                                pendings = s;
                            }
                        } else {
                            pendings = sync.cond.wait(pendings).unwrap();
                        }
                    }

                    if sync.killed.load(Ordering::Relaxed) {
                        break;
                    }

                    // Find the highest possible sync lsn.
                    let mut sync_lsn = None;
                    let mut sync_recs = vec![];
                    while let Some(p) = pendings.front() {
                        if p.sync_time <= Instant::now() {
                            let p = pendings.pop_front().unwrap();
                            sync_lsn = Some(p.lsn);
                            sync_recs.push(MemLogRecord {
                                lsn: p.lsn,
                                op: p.op,
                            });
                        } else {
                            break;
                        }
                    }

                    stor.records.lock().unwrap().extend(sync_recs);
                    let sync_lsn = sync_lsn.expect("guaranteed by cond var");
                    sync.sink.get().expect("should init first")(sync_lsn);
                }
            }
        });

        *sync.thread.lock().unwrap() = Some(thread);
        Self { sync, stor }
    }
}

impl<Op> LogWrite<Op> for WriteImpl<Op>
where
    Op: Send + Sync + Clone + 'static,
{
    fn init(&mut self, sink: Box<dyn Send + Sync + Fn(LSN)>) {
        self.sync
            .sink
            .set(sink)
            .map_err(|_| ())
            .expect("already init");
    }

    fn fire_write(&mut self, op: &Op, lsn: LSN) {
        let mut pendings = self.sync.pendings.lock().unwrap();
        pendings.push_back(PendingLog {
            lsn,
            op: op.clone(),
            sync_time: Instant::now() + self.stor.sync_delay,
        });

        self.sync.cond.notify_one();
    }
}

impl<Op> LogRead<Op> for ReadDiscardImpl<Op>
where
    Op: Send + Sync + Clone + 'static,
{
    fn read(&mut self, start: LSN) -> Result<Box<dyn Iterator<Item = (Op, LSN)>>> {
        let records = self.stor.records.lock().unwrap();
        let res_vec = records
            .iter()
            .filter(|rec| rec.lsn >= start)
            .map(|rec| (rec.op.clone(), rec.lsn))
            .collect::<Vec<_>>();

        Ok(Box::new(res_vec.into_iter()))
    }
}

impl<Op> LogDiscard for ReadDiscardImpl<Op>
where
    Op: Send + Sync + Clone + 'static,
{
    fn fire_discard(&mut self, lsn: LSN) {
        let mut records = self.stor.records.lock().unwrap();
        while let Some(rec) = records.front() {
            if rec.lsn <= lsn {
                records.pop_front();
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
        Self {
            write: Box::new(WriteImpl::<Op>::new(mem.0.clone())),
            read: Box::new(ReadDiscardImpl::<Op>::new(mem.0.clone())),
            discard: Box::new(ReadDiscardImpl::<Op>::new(mem.0.clone())),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{thread::sleep, time::Duration, vec};

    use anyhow::Result;

    use crate::{check_non_blocking, LogCtx, MemLogStorage, LSN};

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

        check_non_blocking(|| ctx.write.fire_write(&TestOp { no: 1 }, 1));
        check_non_blocking(|| ctx.write.fire_write(&TestOp { no: 2 }, 2));

        expect_lsn_sync(2);

        {
            let read_res = ctx.read.read(0).unwrap().collect::<Vec<_>>();
            assert_eq!(read_res, vec![(TestOp { no: 1 }, 1), (TestOp { no: 2 }, 2)]);
        }

        {
            let read_res = ctx.read.read(2).unwrap().collect::<Vec<_>>();
            assert_eq!(read_res, vec![(TestOp { no: 2 }, 2)]);
        }

        check_non_blocking(|| ctx.discard.fire_discard(1));

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

    #[test]
    fn mem_log_durable() -> Result<()> {
        let mut stor = MemLogStorage::new(Duration::from_millis(50));

        {
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

            check_non_blocking(|| ctx.write.fire_write(&TestOp { no: 4 }, 4));
            check_non_blocking(|| ctx.write.fire_write(&TestOp { no: 5 }, 5));

            expect_lsn_sync(5);

            check_non_blocking(|| drop(ctx));
        }

        {
            let mut ctx = LogCtx::<TestOp>::memory(&mut stor);
            let read_res = ctx.read.read(4)?.collect::<Vec<_>>();
            assert_eq!(read_res, vec![(TestOp { no: 4 }, 4), (TestOp { no: 5 }, 5)]);
        }

        Ok(())
    }
}
