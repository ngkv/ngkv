use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex,
    },
    thread::{spawn, JoinHandle},
    time::{Duration, Instant},
};

use crate::{LogCtx, LogDiscard, LogRead, LogRecord, LogWrite, LogWriteOptions, Lsn, Result};
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
    lsn: Lsn,
    op: Op,
}

struct PendingLog<Op> {
    op: Op,
    lsn: Lsn,
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
    sink: OnceCell<Box<dyn Send + Sync + Fn(Lsn)>>,
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
    _sync_thread: JoinHandle<()>,
    stor: Arc<StorageShared<Op>>,
}

impl<Op> Drop for WriteImpl<Op> {
    fn drop(&mut self) {
        self.sync.killed.store(true, Ordering::Relaxed);
        self.sync.cond.notify_one();

        // Join thread here (by dropping _sync_thread). This ensures that sync
        // sink would not be called after drop.
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

        Self {
            sync,
            stor,
            _sync_thread: thread,
        }
    }
}

impl<Op> LogWrite<Op> for WriteImpl<Op>
where
    Op: Send + Sync + Clone + 'static,
{
    fn init(&mut self, _start_lsn: Lsn, sink: Box<dyn Send + Sync + Fn(Lsn)>) -> Result<()> {
        self.sync
            .sink
            .set(sink)
            .map_err(|_| ())
            .expect("already init");
        Ok(())
    }

    fn fire_write(&mut self, rec: &LogRecord<Op>, _options: &LogWriteOptions) -> Result<()> {
        let mut pendings = self.sync.pendings.lock().unwrap();
        pendings.push_back(PendingLog {
            lsn: rec.lsn,
            op: rec.op.clone(),
            sync_time: Instant::now() + self.stor.sync_delay,
        });
        self.sync.cond.notify_one();
        Ok(())
    }
}

impl<Op> LogRead<Op> for ReadDiscardImpl<Op>
where
    Op: Send + Sync + Clone + 'static,
{
    fn read(&mut self, start: Lsn) -> Result<Box<dyn Iterator<Item = LogRecord<Op>>>> {
        let records = self.stor.records.lock().unwrap();
        let res_vec = records
            .iter()
            .filter(|rec| rec.lsn >= start)
            .map(|rec| LogRecord {
                op: rec.op.clone(),
                lsn: rec.lsn,
            })
            .collect::<Vec<_>>();

        Ok(Box::new(res_vec.into_iter()))
    }
}

impl<Op> LogDiscard for ReadDiscardImpl<Op>
where
    Op: Send + Sync + Clone + 'static,
{
    fn fire_discard(&mut self, lsn: Lsn) -> Result<()> {
        let mut records = self.stor.records.lock().unwrap();
        while let Some(rec) = records.front() {
            if rec.lsn <= lsn {
                records.pop_front();
            } else {
                break;
            }
        }

        Ok(())
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
    use crate::{
        tests::{assert_test_op_iter, check_non_blocking, test_op_write, LogSyncWait, TestOp},
        LogCtx, LogWriteOptions, MemLogStorage, Result,
    };
    use std::{thread::sleep, time::Duration, vec};

    #[test]
    fn mem_log_normal() -> Result<()> {
        let mut stor = MemLogStorage::new(Duration::from_millis(50));

        let mut ctx = LogCtx::<TestOp>::memory(&mut stor);
        let (mut sync_wait, sync_sink) = LogSyncWait::create();
        ctx.write.init(1, sync_sink)?;

        test_op_write(&mut *ctx.write, 1, &LogWriteOptions::default());
        test_op_write(&mut *ctx.write, 2, &LogWriteOptions::default());

        sync_wait.wait(2);

        {
            let read_res = ctx.read.read(0).unwrap().collect::<Vec<_>>();
            assert_eq!(read_res.len(), 2);
            assert_eq!(read_res[0].lsn, 1);
            assert_test_op_iter(read_res.iter());
        }

        {
            let read_res = ctx.read.read(2).unwrap().collect::<Vec<_>>();
            assert_eq!(read_res.len(), 1);
            assert_eq!(read_res[0].lsn, 2);
            assert_test_op_iter(read_res.iter());
        }

        check_non_blocking(|| ctx.discard.fire_discard(1).unwrap());

        {
            let read_res = ctx.read.read(0).unwrap().collect::<Vec<_>>();
            assert_eq!(read_res.len(), 1);
            assert_eq!(read_res[0].lsn, 2);
            assert_test_op_iter(read_res.iter());
        }

        Ok(())
    }

    #[test]
    fn mem_log_drop_no_sync() -> Result<()> {
        let mut stor = MemLogStorage::new(Duration::from_millis(50));

        {
            let mut ctx = LogCtx::<TestOp>::memory(&mut stor);
            ctx.write.init(1, Box::new(|_| {}))?;
            test_op_write(&mut *ctx.write, 1, &LogWriteOptions::default());
            test_op_write(&mut *ctx.write, 2, &LogWriteOptions::default());
        }

        // wait at least sync_delay
        sleep(Duration::from_millis(100));

        {
            let mut ctx = LogCtx::<TestOp>::memory(&mut stor);
            let read_res = ctx.read.read(0).unwrap().collect::<Vec<_>>();
            assert_eq!(read_res, vec![]);
        }

        Ok(())
    }

    #[test]
    fn mem_log_durable() -> Result<()> {
        let mut stor = MemLogStorage::new(Duration::from_millis(50));

        {
            let mut ctx = LogCtx::<TestOp>::memory(&mut stor);
            let (mut sync_wait, sync_sink) = LogSyncWait::create();
            ctx.write.init(1, sync_sink)?;
            test_op_write(&mut *ctx.write, 4, &LogWriteOptions::default());
            test_op_write(&mut *ctx.write, 5, &LogWriteOptions::default());

            sync_wait.wait(5);

            check_non_blocking(|| drop(ctx));
        }

        {
            let mut ctx = LogCtx::<TestOp>::memory(&mut stor);
            let read_res = ctx.read.read(4)?.collect::<Vec<_>>();
            assert_eq!(read_res.len(), 2);
            assert_eq!(read_res[0].lsn, 4);
            assert_test_op_iter(read_res.iter());
        }

        Ok(())
    }
}
