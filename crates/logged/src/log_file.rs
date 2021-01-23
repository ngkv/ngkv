use std::{
    cmp::{max, Ord},
    convert::TryFrom,
    fs::{read_dir, File, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
    marker::PhantomData,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex, MutexGuard,
    },
    thread::{spawn, JoinHandle},
    time::{Duration, Instant},
};

use anyhow::{anyhow, bail, ensure, Context, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use once_cell::sync::OnceCell;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    crc32_io::{Crc32Read, Crc32Write},
    LogCtx, LogDiscard, LogRead, LogWrite, LogWriteOptions, LSN,
};

#[derive(Clone)]
pub struct FileLogOptions {
    pub dir: PathBuf,
    pub sync_policy: FileSyncPolicy,
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

// Read a record from reader.
//
// Note that it never fails, corrupted entry & all entries after it are
// discarded.
fn read_record<Op: DeserializeOwned>(mut reader: impl Read) -> Option<Op> {
    let result = || -> Result<Op> {
        // Following reads (until CRC itself) would be covered by CRC.
        let mut reader_crc = Crc32Read::new(&mut reader);

        // Read magic & compare.
        let magic = reader_crc.read_u8()?;
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

        Ok(op)
    }();

    match result {
        Ok(op) => Some(op),
        Err(_e) => {
            // TODO: log
            None
        }
    }
}

enum WriteRecordStatus {
    Done(u64),          // Write record succeeded. Return how many bytes have been written.
    NoEnoughSpace(u64), // No enouph space in log file. Return how many bytes this record would take.
}

// Serialize an op. Works together with write_record.
fn serialize_op<Op: Serialize>(op: &Op) -> Result<Vec<u8>> {
    let buf = bincode::serialize(op)?;
    Ok(buf)
}

// Write a record to writer.
fn write_record(
    mut writer: impl Write,
    space_remain: u64,
    payload_buf: &[u8],
) -> Result<WriteRecordStatus> {
    let space = 10 + payload_buf.len() as u64;
    if space > space_remain {
        return Ok(WriteRecordStatus::NoEnoughSpace(space));
    }

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

    Ok(WriteRecordStatus::Done(space))
}

// Log File Name Format
// log-{start}-{end}

// Return start LSN
fn parse_file_name(name: &str) -> Result<LSN> {
    let fields = name.split("-").collect::<Vec<_>>();
    ensure!(
        fields.len() == 2 && fields[0] == "log",
        "invalid log filename"
    );

    Ok(fields[1].parse()?)
}

fn make_file_name(start_lsn: LSN) -> String {
    format!("log-{}", start_lsn)
}

struct ReadLogIter<R, Op> {
    reader: R,
    next_lsn: LSN,
    _op: PhantomData<Op>,
}

impl<R, Op> Iterator for ReadLogIter<R, Op>
where
    R: Read,
    Op: DeserializeOwned,
{
    type Item = (Op, LSN);

    fn next(&mut self) -> Option<Self::Item> {
        let op = read_record(&mut self.reader)?;
        let lsn = self.next_lsn;
        self.next_lsn += 1;
        Some((op, lsn))
    }
}

struct FileInfo {
    start_lsn: LSN,
    file: std::fs::File,
}

impl FileInfo {
    fn iter_logs<Op>(self) -> impl Iterator<Item = (Op, LSN)>
    where
        Op: DeserializeOwned,
    {
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
    fn read(&mut self, start: LSN) -> Result<Box<dyn Iterator<Item = (Op, LSN)>>> {
        let mut files = vec![];

        // Find log files we interested in.
        for ent in read_dir(&self.dir)? {
            let ent = ent?;
            let os_name = ent.file_name();
            let name = os_name.to_str().ok_or(anyhow!("invalid log filename"))?;
            let start_lsn = parse_file_name(name)?;
            files.push(FileInfo {
                start_lsn,
                file: OpenOptions::new().read(true).open(ent.path())?,
            });
        }

        // Sort them by ascending order.
        files.sort_by(|a, b| Ord::cmp(&a.start_lsn, &b.start_lsn));

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

// struct PendingWrite<Op> {
//     op: Op,
//     lsn: LSN,
//     force_sync: bool,
// }

struct SingleLogFile {
    written: u64,
    writer: BufWriter<File>,
    fd: File,
    size: u64,
    request_force_sync: bool,
    sync_lsn: LSN,
    pending_lsn: LSN,
}

struct WriteSyncState {
    next_sync_time: Option<Instant>,
    // cur_file: File,

    // cur_writer: BufWriter<File>,
    // cur_pendings: VecDeque<PendingWrite<Op>>,
    cur_file: SingleLogFile,
    // prev_file: Option<SingleFilePendingState>,
    // cur_pendings: VecDeque<PendingWrite<Op>>,
    // prev_file: Option<File>,
    // prev_pendings: Option<VecDeque<PendingWrite<Op>>>,
}

struct WriteSyncShared {
    options: FileLogOptions,
    cond: Condvar,
    killed: AtomicBool,
    failed: AtomicBool,
    sink: Box<dyn Send + Sync + Fn(LSN)>,
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
    fn open_log_file(start_lsn: LSN, options: &FileLogOptions) -> Result<SingleLogFile> {
        let path = make_file_name(start_lsn);
        let file = OpenOptions::new().write(true).truncate(true).open(path)?;
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
        while !sync.killed.load(Ordering::Relaxed) {
            let next_sync_time = state.next_sync_time.expect("set during thread spawn");
            let now = Instant::now();

            // Any pending write?
            if state.cur_file.pending_lsn > state.cur_file.sync_lsn {
                // We have pending writes. Sync them when:
                // 1. Asked by the write options (i.e. force_sync flag).
                // 2. Asked by the periodical sync policy.
                let case_sync_time = || now >= next_sync_time;
                let case_force_sync = || state.cur_file.request_force_sync;
                if case_sync_time() || case_force_sync() {
                    // We should sync now.
                    break;
                } else {
                    // Wait for periodical sync.
                    let (s, _) = sync.cond.wait_timeout(state, next_sync_time - now).unwrap();
                    state = s;
                }
            } else {
                state = sync.cond.wait(state).unwrap();
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
        if state.cur_file.size >= sync.options.log_switch_size {
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
                        if sync.killed.load(Ordering::Relaxed) {
                            break;
                        }

                        // Not killed, perform the sync.
                        state.next_sync_time = Some(Instant::now() + dur);

                        match Self::do_sync(&*sync, state) {
                            Ok(_) => {
                                // Re-aquire lock.
                                state = sync.state.lock().unwrap();
                            }
                            Err(e) => {
                                // TODO: log sync thread failed.
                                sync.failed.store(true, Ordering::Release);
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
        start_lsn: LSN,
        sink: Box<dyn Send + Sync + Fn(LSN)>,
        options: FileLogOptions,
    ) -> Result<Self> {
        let cur_file = Self::open_log_file(start_lsn, &options)?;

        let state = WriteSyncState {
            next_sync_time: None,
            cur_file,
        };

        let shared = Arc::new(WriteSyncShared {
            options,
            killed: AtomicBool::new(false),
            failed: AtomicBool::new(false),
            cond: Default::default(),
            sink,
            state: Mutex::new(state),
        });

        Ok(Self {
            sync_thread: Some(Self::spawn_sync_thread(shared.clone())),
            sync_shared: shared,
            _op: PhantomData::default(),
        })
    }

    fn fire_write(&mut self, op: &Op, lsn: LSN, options: &LogWriteOptions) -> Result<()> {
        // Serialize ahead of time to avoid duplicated serialization.
        let op_buf = serialize_op(op)?;

        loop {
            let failed = self.sync_shared.failed.load(Ordering::Acquire);
            if failed {
                if let Some(t) = self.sync_thread.take() {
                    // Get error produced by sync thread.
                    let res = t.join().expect("join thread failed");
                    bail!(res.expect_err("should be err when failed"));
                } else {
                    bail!("unable to sync due to previous error");
                }
            }

            let mut state = self.sync_shared.state.lock().unwrap();
            let space_remain = state.cur_file.size - state.cur_file.written;
            let status = write_record(&mut state.cur_file.writer, space_remain, &op_buf)?;
            match status {
                WriteRecordStatus::Done(written) => {
                    state.cur_file.written += written;
                    state.cur_file.pending_lsn = lsn;
                    state.cur_file.request_force_sync |= options.force_sync;
                    break;
                }
                WriteRecordStatus::NoEnoughSpace(space) => {
                    let target_size = max(
                        state.cur_file.size + self.sync_shared.options.log_step_size,
                        state.cur_file.written + space,
                    );
                    Self::enlarge_log_file(&mut state.cur_file, target_size)?;
                }
            }
        }

        Ok(())
    }
}

impl<Op> Drop for WriteInner<Op> {
    fn drop(&mut self) {
        self.sync_shared.killed.store(true, Ordering::Relaxed);
        self.sync_shared.cond.notify_one();
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
    fn init(&mut self, start_lsn: LSN, sink: Box<dyn Send + Sync + Fn(LSN)>) -> Result<()> {
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

    fn fire_write(&mut self, op: &Op, lsn: LSN, options: &LogWriteOptions) -> Result<()> {
        self.inner
            .get_mut()
            .expect("not init")
            .fire_write(op, lsn, options)
    }
}

struct DiscardImpl {}

impl LogDiscard for DiscardImpl {
    fn fire_discard(&mut self, _lsn: LSN) -> Result<()> {
        // TODO: implement log discard
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
            discard: Box::new(DiscardImpl {}),
        }
    }
}
