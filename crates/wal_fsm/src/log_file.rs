use std::{
    cmp::{max, min, Ord},
    convert::TryFrom,
    fs::{read_dir, remove_file, File, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex, MutexGuard},
    thread::{spawn, JoinHandle},
    time::{Duration, Instant},
};

use anyhow::{anyhow, bail, ensure, Context, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use itertools::Itertools;
use once_cell::sync::OnceCell;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    crc32_io::{Crc32Read, Crc32Write},
    LogCtx, LogDiscard, LogRead, LogRecord, LogWrite, LogWriteOptions, Lsn,
};

#[derive(Clone)]
pub struct FileLogOptions {
    pub dir: PathBuf,
    pub sync_policy: FileSyncPolicy,
    pub max_pending: u64,
    pub log_step_size: u64,
    pub log_switch_size: u64,
}

#[derive(Clone)]
pub enum FileSyncPolicy {
    Periodical(Duration),
}

// Physical Representation of LogEntry:
// Magic 0xef: u8
// Type: u8
// Payload length: u32
// Payload: [u8]
// Checksum (of previous fields): u32

const MAGIC: u8 = 0xef;

#[repr(u8)]
enum LogEntryType {
    Default = 0, // bincode encoding
}

impl TryFrom<u8> for LogEntryType {
    type Error = anyhow::Error;

    fn try_from(v: u8) -> anyhow::Result<Self> {
        match v {
            x if x == LogEntryType::Default as u8 => Ok(LogEntryType::Default),
            x => Err(anyhow!("invalid log entry type {}", x)),
        }
    }
}

// Read all log files from a directory. Files are ordered by start lsn.
fn read_dir_logs(dir: &Path) -> Result<Vec<FileInfo>> {
    // Find log files we interested in.
    let mut files = vec![];
    for ent in read_dir(dir)? {
        let ent = ent?;
        let os_name = ent.file_name();
        let name = os_name.to_str().ok_or(anyhow!("invalid log filename"))?;
        let start_lsn = parse_file_name(name)?;
        files.push(FileInfo {
            start_lsn,
            path: ent.path(),
            file: OpenOptions::new().read(true).open(ent.path())?,
        });
    }

    // Sort them by ascending order.
    files.sort_by(|a, b| Ord::cmp(&a.start_lsn, &b.start_lsn));

    Ok(files)
}

// Read a record from reader.
//
// Note that it never fails, corrupted entry & all entries after it are
// discarded.
fn read_record<Op: DeserializeOwned>(mut reader: impl Read) -> Option<Op> {
    let result = || -> Result<Option<Op>> {
        // Following reads (until CRC itself) would be covered by CRC.
        let mut reader_crc = Crc32Read::new(&mut reader);

        let magic = match reader_crc.read_u8() {
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            x => x?,
        };
        ensure!(magic == MAGIC, "magic mismatch");

        // Read type.
        let typ = LogEntryType::try_from(reader_crc.read_u8()?)?;

        // Read payload.
        let payload_len = reader_crc.read_u32::<LittleEndian>()?;
        let mut payload_buf = vec![0 as u8; payload_len as usize];
        reader_crc.read_exact(&mut payload_buf)?;

        // Compare CRC.
        let crc_actual = reader_crc.finalize();
        let crc_expect = reader.read_u32::<LittleEndian>()?;
        ensure!(crc_actual == crc_expect, "crc mismatch");

        let op: Op = match typ {
            LogEntryType::Default => bincode::deserialize(&payload_buf)?,
        };

        Ok(Some(op))
    }();

    match result {
        Ok(op) => op,
        Err(e) => {
            log::info!("log record read error: {}", e);
            None
        }
    }
}

// Serialize an op. Works together with write_record.
fn serialize_op<Op: Serialize>(op: &Op) -> Result<Vec<u8>> {
    let buf = bincode::serialize(op)?;
    Ok(buf)
}

fn record_size(payload_size: u64) -> u64 {
    10 + payload_size
}

// Write a record to writer.
fn write_record(mut writer: impl Write, payload_buf: &[u8]) -> Result<()> {
    // Following writes (until CRC itself) would be covered by CRC.
    let mut writer_crc = Crc32Write::new(&mut writer);

    // Write magic.
    writer_crc.write_u8(MAGIC)?;

    // Write type.
    writer_crc.write_u8(LogEntryType::Default as u8)?;

    // Write payload.
    let payload_len = u32::try_from(payload_buf.len()).with_context(|| "payload too long")?;
    writer_crc.write_u32::<LittleEndian>(payload_len)?;
    writer_crc.write_all(&payload_buf)?;

    // Write CRC.
    let crc = writer_crc.finalize();
    writer.write_u32::<LittleEndian>(crc)?;

    Ok(())
}

// Log File Name Format
// log-{start}

// Return start LSN
fn parse_file_name(name: &str) -> Result<Lsn> {
    let fields = name.split("-").collect::<Vec<_>>();
    ensure!(
        fields.len() == 2 && fields[0] == "log",
        "invalid log filename"
    );

    Ok(fields[1].parse()?)
}

fn make_file_name(start_lsn: Lsn) -> String {
    format!("log-{}", start_lsn)
}

struct ReadLogIter<R, Op> {
    reader: R,
    next_lsn: Lsn,
    _op: PhantomData<Op>,
}

impl<R, Op> Iterator for ReadLogIter<R, Op>
where
    R: Read,
    Op: DeserializeOwned,
{
    type Item = LogRecord<Op>;

    fn next(&mut self) -> Option<Self::Item> {
        let lsn = self.next_lsn;
        log::debug!("trying to read record {}", lsn);
        let op = read_record(&mut self.reader)?;
        self.next_lsn += 1;
        Some(LogRecord { op, lsn })
    }
}

struct FileInfo {
    start_lsn: Lsn,
    file: File,
    path: PathBuf,
}

impl FileInfo {
    fn iter_logs<Op>(self) -> impl Iterator<Item = LogRecord<Op>>
    where
        Op: DeserializeOwned,
    {
        log::info!("iterating file {}", self.start_lsn);
        let reader = BufReader::new(self.file);
        ReadLogIter {
            reader,
            next_lsn: self.start_lsn,
            _op: PhantomData::default(),
        }
    }
}

struct ReadImpl<Op> {
    dir: PathBuf,
    _op: PhantomData<Op>,
}

impl<Op> LogRead<Op> for ReadImpl<Op>
where
    Op: Send + Sync + DeserializeOwned + 'static,
{
    fn read(&mut self, start: Lsn) -> Result<Box<dyn Iterator<Item = LogRecord<Op>>>> {
        let files = read_dir_logs(&self.dir)?;

        let (higher, lower): (Vec<_>, Vec<_>) =
            files.into_iter().partition(|f| f.start_lsn >= start);

        // Related log files consists two parts:
        // 1. Last log file with lower start LSN, if exists.
        // 2. All log files with higher start LSN.
        let mut related_files = vec![];
        if !lower.is_empty() {
            let mut lower = lower;
            related_files.push(lower.remove(lower.len() - 1));
        }
        related_files.extend(higher);

        // Iterate over files, flat map logs.
        Ok(Box::new(
            related_files.into_iter().flat_map(|f| f.iter_logs()),
        ))
    }
}

struct SingleLogFile {
    written: u64,
    writer: BufWriter<File>,
    fd: File,
    size: u64,
    request_force_sync: bool,
    sync_lsn: Lsn,
    pending_lsn: Lsn,
}

struct WriteSyncState {
    next_sync_time: Option<Instant>,
    cur_file: SingleLogFile,
    killed: bool,
    failed: bool,
}

struct WriteSyncShared {
    options: FileLogOptions,
    cv_has_pending: Condvar,
    cv_log_switch: Condvar,
    // killed: AtomicBool,
    // failed: AtomicBool,
    sink: Box<dyn Send + Sync + Fn(Lsn)>,
    state: Mutex<WriteSyncState>,
}

struct WriteInner<Op> {
    sync_thread: Option<JoinHandle<Result<()>>>,
    sync_shared: Arc<WriteSyncShared>,
    _op: PhantomData<Op>,
}

impl<Op> WriteInner<Op>
where
    Op: Send + Sync + Clone + Serialize + 'static,
{
    fn open_log_file(start_lsn: Lsn, options: &FileLogOptions) -> Result<SingleLogFile> {
        log::info!("opening new wal file {}", start_lsn);
        let name = make_file_name(start_lsn);

        let mut path = options.dir.clone();
        path.push(name);

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        file.set_len(options.log_step_size)?;

        Ok(SingleLogFile {
            written: 0,
            writer: BufWriter::new(file.try_clone()?),
            fd: file,
            pending_lsn: 0,
            sync_lsn: 0,
            size: options.log_step_size,
            request_force_sync: false,
        })
    }

    fn enlarge_log_file(file: &mut SingleLogFile, size: u64) -> Result<()> {
        file.fd.set_len(size)?;
        file.size = size;
        Ok(())
    }

    // Wait for kill or sync. Sync is only possible while there are pending
    // writes.
    fn wait_for_kill_or_sync<'a>(
        sync: &WriteSyncShared,
        mut state: MutexGuard<'a, WriteSyncState>,
    ) -> MutexGuard<'a, WriteSyncState> {
        while !state.killed {
            let next_sync_time = state.next_sync_time.expect("set during thread spawn");
            let now = Instant::now();

            // Any pending write?
            if state.cur_file.pending_lsn > state.cur_file.sync_lsn {
                // We have pending writes. Sync them when:
                // 1. Asked by the write options (i.e. force_sync flag).
                // 2. Time to do periodical sync.
                // 3. Log file should be switched.
                let case_sync_time = now >= next_sync_time;
                let case_force_sync = state.cur_file.request_force_sync;
                let case_log_switch = state.cur_file.written >= sync.options.log_switch_size;
                log::debug!(
                    "sync_time: {}, force_sync: {}, log_switch: {}",
                    case_sync_time,
                    case_force_sync,
                    case_log_switch
                );
                if case_sync_time || case_force_sync || case_log_switch {
                    // We should sync now.
                    break;
                } else {
                    // Wait for periodical sync.
                    let (s, _) = sync
                        .cv_has_pending
                        .wait_timeout(state, next_sync_time - now)
                        .unwrap();
                    state = s;
                }
            } else {
                state = sync.cv_has_pending.wait(state).unwrap();
            }
        }

        state
    }

    fn do_sync(sync: &WriteSyncShared, mut state: MutexGuard<WriteSyncState>) -> Result<()> {
        let sync_fd = state.cur_file.fd.try_clone()?;
        let sync_lsn = state.cur_file.pending_lsn;

        // Sanity check: Are there any pending writes?
        assert!(state.cur_file.pending_lsn > state.cur_file.sync_lsn);

        // Update state for sync before unlocking. This does no harm, since we
        // notify the external world by calling the sync sink, and it happens
        // after the actual sync.
        state.cur_file.request_force_sync = false;
        state.cur_file.sync_lsn = state.cur_file.pending_lsn;
        state.cur_file.writer.flush()?;

        // Check if we should switch log file.
        if state.cur_file.written >= sync.options.log_switch_size {
            assert_ne!(state.cur_file.pending_lsn, 0);
            let new_file = Self::open_log_file(state.cur_file.pending_lsn + 1, &sync.options)?;
            let _old = std::mem::replace(&mut state.cur_file, new_file);
        }

        // Unlock before syncing.
        drop(state);

        // Sync & notify external world. No race here because we are in the only
        // sync thread.
        sync_fd.sync_all()?;
        (sync.sink)(sync_lsn);

        Ok(())
    }

    fn spawn_sync_thread(sync: Arc<WriteSyncShared>) -> JoinHandle<Result<()>> {
        spawn(move || -> _ {
            match sync.options.sync_policy {
                FileSyncPolicy::Periodical(dur) => {
                    let mut state = sync.state.lock().unwrap();

                    // Set initial sync time.
                    if state.next_sync_time.is_none() {
                        state.next_sync_time = Some(Instant::now() + dur);
                    }

                    loop {
                        state = Self::wait_for_kill_or_sync(&*sync, state);

                        // Killed?
                        if state.killed {
                            break;
                        }

                        // Not killed, perform the sync.
                        state.next_sync_time = Some(Instant::now() + dur);

                        let res = Self::do_sync(&*sync, state);

                        // Re-aquire lock.
                        state = sync.state.lock().unwrap();

                        match res {
                            Ok(_) => {
                                sync.cv_log_switch.notify_all();
                            }
                            Err(e) => {
                                log::error!("sync failed: {}", e);
                                state.failed = true;
                                sync.cv_log_switch.notify_all();
                                return Err(e);
                            }
                        }
                    }

                    Ok(())
                }
            }
        })
    }

    fn new(
        start_lsn: Lsn,
        sink: Box<dyn Send + Sync + Fn(Lsn)>,
        options: FileLogOptions,
    ) -> Result<Self> {
        let cur_file = Self::open_log_file(start_lsn, &options)?;

        let state = WriteSyncState {
            next_sync_time: None,
            cur_file,
            failed: false,
            killed: false,
        };

        let shared = Arc::new(WriteSyncShared {
            options,
            cv_has_pending: Default::default(),
            cv_log_switch: Default::default(),
            sink,
            state: Mutex::new(state),
        });

        Ok(Self {
            sync_thread: Some(Self::spawn_sync_thread(shared.clone())),
            sync_shared: shared,
            _op: PhantomData::default(),
        })
    }

    fn wait_for_fail_or_writable<'a>(
        sync: &WriteSyncShared,
        mut state: MutexGuard<'a, WriteSyncState>,
    ) -> MutexGuard<'a, WriteSyncState> {
        while !state.failed && state.cur_file.written >= sync.options.log_switch_size {
            log::debug!("waiting for log switch, size={}", state.cur_file.size);
            state = sync.cv_log_switch.wait(state).unwrap();
        }
        state
    }

    fn fire_write(&mut self, op: &Op, lsn: Lsn, options: &LogWriteOptions) -> Result<()> {
        // Serialize ahead of time to avoid duplicated serialization.
        let op_buf = serialize_op(op)?;
        let mut state = self.sync_shared.state.lock().unwrap();

        log::debug!("firing write {}", lsn);
        state = Self::wait_for_fail_or_writable(&*self.sync_shared, state);

        if state.failed {
            if let Some(t) = self.sync_thread.take() {
                drop(state);
                // Get error produced by sync thread.
                let res = t.join().expect("join thread failed");
                bail!(res.expect_err("should be err when failed"));
            } else {
                bail!("unable to sync due to previous error");
            }
        }

        assert!(state.cur_file.written < self.sync_shared.options.log_switch_size);

        let space_remain = state.cur_file.size - state.cur_file.written;
        let rec_size = record_size(op_buf.len() as u64);
        if space_remain < rec_size {
            let target_size = max(
                min(
                    state.cur_file.size + self.sync_shared.options.log_step_size,
                    self.sync_shared.options.log_switch_size,
                ),
                state.cur_file.written + rec_size,
            );
            Self::enlarge_log_file(&mut state.cur_file, target_size)?;
        }

        write_record(&mut state.cur_file.writer, &op_buf)?;
        state.cur_file.written += rec_size;
        state.cur_file.pending_lsn = lsn;
        state.cur_file.request_force_sync |= options.force_sync;

        self.sync_shared.cv_has_pending.notify_one();

        Ok(())
    }
}

impl<Op> Drop for WriteInner<Op> {
    fn drop(&mut self) {
        let mut state = self.sync_shared.state.lock().unwrap();
        state.killed = true;
        self.sync_shared.cv_has_pending.notify_one();
    }
}

struct WriteImpl<Op> {
    options: Option<FileLogOptions>,
    inner: OnceCell<WriteInner<Op>>,
}

impl<Op> LogWrite<Op> for WriteImpl<Op>
where
    Op: Send + Sync + Clone + Serialize + 'static,
{
    fn init(&mut self, start_lsn: Lsn, sink: Box<dyn Send + Sync + Fn(Lsn)>) -> Result<()> {
        self.inner
            .set(WriteInner::new(
                start_lsn,
                sink,
                self.options.take().unwrap(),
            )?)
            .map_err(|_| ())
            .expect("already init");
        Ok(())
    }

    fn fire_write(&mut self, rec: &LogRecord<Op>, options: &LogWriteOptions) -> Result<()> {
        self.inner
            .get_mut()
            .expect("not init")
            .fire_write(&rec.op, rec.lsn, options)
    }
}

struct DiscardImpl {
    dir: PathBuf,
}

impl LogDiscard for DiscardImpl {
    fn fire_discard(&mut self, lsn: Lsn) -> Result<()> {
        // Find all files whose start_lsn <= lsn
        let files = read_dir_logs(&self.dir)?;
        let mut lower = files
            .into_iter()
            .filter(|f| f.start_lsn <= lsn)
            .collect_vec();

        // All lower file except last one could be safely discarded.
        if !lower.is_empty() {
            lower.remove(lower.len() - 1);
        }

        for f in lower.into_iter() {
            remove_file(&f.path)?;
        }

        Ok(())
    }
}

impl<Op> LogCtx<Op>
where
    Op: Send + Sync + Clone + Serialize + DeserializeOwned + 'static,
{
    pub fn file(options: &FileLogOptions) -> Self {
        Self {
            read: Box::new(ReadImpl {
                dir: options.dir.clone(),
                _op: PhantomData::default(),
            }),
            write: Box::new(WriteImpl {
                inner: OnceCell::new(),
                options: Some(options.clone()),
            }),
            discard: Box::new(DiscardImpl {
                dir: options.dir.clone(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        tests::{assert_test_op_iter, check_non_blocking, test_op_write, LogSyncWait, TestOp},
        FileLogOptions, FileSyncPolicy, LogCtx, LogWriteOptions, Lsn,
    };
    use anyhow::Result;
    use itertools::Itertools;
    use std::{
        path::{Path, PathBuf},
        thread::sleep,
        time::{Duration, Instant},
    };
    use tempdir::TempDir;

    use super::{make_file_name, parse_file_name};

    fn temp_dir() -> Result<TempDir> {
        let dir = TempDir::new("wal_fsm")?;
        Ok(dir)
    }

    fn get_dir_lsns(dir: &Path) -> Result<Vec<Lsn>> {
        let mut res = std::fs::read_dir(&dir)?
            .filter_map(|ent| {
                let ent = ent.ok()?;
                let os_name = ent.file_name();
                parse_file_name(os_name.to_str()?).ok()
            })
            .collect_vec();
        res.sort();
        Ok(res)
    }

    fn test_option_small(dir: &Path) -> FileLogOptions {
        FileLogOptions {
            dir: PathBuf::from(dir),
            log_step_size: 64,
            log_switch_size: 128,
            max_pending: 10,
            sync_policy: FileSyncPolicy::Periodical(Duration::from_secs(1)),
        }
    }

    #[test]
    fn file_log_write_close_read() {
        let dir = temp_dir().unwrap();
        let options = test_option_small(dir.path());

        {
            let mut ctx = LogCtx::<TestOp>::file(&options);
            let (mut sync_wait, sync_sink) = LogSyncWait::create();
            ctx.write.init(1, sync_sink).unwrap();

            for i in 1..=3 {
                test_op_write(&mut *ctx.write, i, &LogWriteOptions { force_sync: i == 3 });
            }

            sync_wait.wait(3);
        }

        {
            let mut ctx = LogCtx::<TestOp>::file(&options);
            let read_res = ctx.read.read(0).unwrap().collect_vec();
            assert_eq!(read_res.len(), 3);
            assert_eq!(read_res[0].lsn, 1);
            assert_test_op_iter(read_res.iter());
        }
    }

    #[test]
    fn file_log_periodic_sync() {
        let dir = temp_dir().unwrap();
        let options = test_option_small(dir.path());

        {
            let mut ctx = LogCtx::<TestOp>::file(&options);
            let (mut sync_wait, sync_sink) = LogSyncWait::create();
            ctx.write.init(1, sync_sink).unwrap();

            let now = Instant::now();

            for i in 1..=3 {
                test_op_write(&mut *ctx.write, i, &LogWriteOptions { force_sync: false });
            }

            sync_wait.wait(3);

            let time = Instant::now() - now;
            assert!(time >= Duration::from_secs(1));
        }
    }

    #[test]
    fn file_log_switch() {
        let dir = temp_dir().unwrap();
        let options = test_option_small(dir.path());

        {
            let mut ctx = LogCtx::<TestOp>::file(&options);
            let (mut sync_wait, sync_sink) = LogSyncWait::create();
            ctx.write.init(1, sync_sink).unwrap();

            let count = 100;
            for i in 1..=count {
                let force_sync = i == count;
                test_op_write(&mut *ctx.write, i, &LogWriteOptions { force_sync });
            }

            sync_wait.wait(count);
        }

        let lsns = get_dir_lsns(&dir.path()).unwrap();
        assert!(lsns.len() >= 3, "shoule be multiple logs: {:?}", lsns);
    }

    #[test]
    fn file_log_discard() {
        let dir = temp_dir().unwrap();
        let options = test_option_small(dir.path());

        {
            let mut ctx = LogCtx::<TestOp>::file(&options);
            let (mut sync_wait, sync_sink) = LogSyncWait::create();
            ctx.write.init(1, sync_sink).unwrap();

            let count = 100;
            for i in 1..=count {
                let force_sync = i == count;
                test_op_write(&mut *ctx.write, i, &LogWriteOptions { force_sync });
            }

            sync_wait.wait(count);
        }

        let before_discard = get_dir_lsns(&dir.path()).unwrap();

        {
            let mut ctx = LogCtx::<TestOp>::file(&options);
            check_non_blocking(|| {
                ctx.discard.fire_discard(50).unwrap();
            });

            // Wait for actual discard.
            sleep(Duration::from_secs(1));
        }

        let after_discard = get_dir_lsns(&dir.path()).unwrap();

        assert!(
            after_discard.len() < before_discard.len(),
            "after: {:?}, before: {:?}",
            after_discard,
            before_discard
        );

        {
            let mut ctx = LogCtx::<TestOp>::file(&options);
            let read_res = ctx.read.read(50).unwrap().collect_vec();
            assert_test_op_iter(read_res.iter());
            assert!(read_res[0].lsn <= 50);
        }
    }

    #[test]
    fn file_log_corrupt_check() {
        let dir = temp_dir().unwrap();
        let options = test_option_small(dir.path());

        {
            let mut ctx = LogCtx::<TestOp>::file(&options);
            let (mut sync_wait, sync_sink) = LogSyncWait::create();
            ctx.write.init(1, sync_sink).unwrap();

            let count = 2;
            for i in 1..=count {
                let force_sync = i == count;
                test_op_write(&mut *ctx.write, i, &LogWriteOptions { force_sync });
            }

            sync_wait.wait(count);
        }

        // Flip single bit in first log record.
        {
            let name = make_file_name(1);
            let file_path = dir.path().join(name);
            let mut data = std::fs::read(&file_path).unwrap();
            data[7] = data[7] ^ 0x80;
            std::fs::write(&file_path, &data).unwrap();
        }

        {
            let mut ctx = LogCtx::<TestOp>::file(&options);
            let read_res = ctx.read.read(1).unwrap().collect_vec();
            assert_eq!(read_res.len(), 0);
        }
    }

    // TODO: stress test
}
