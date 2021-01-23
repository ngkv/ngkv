use std::{
    cmp::Ord,
    convert::TryFrom,
    fmt::write,
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
    todo,
};

use anyhow::{anyhow, bail, ensure, Context, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crossbeam::channel::unbounded;
use once_cell::sync::OnceCell;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    crc32_io::{Crc32Read, Crc32Write},
    log_ctx, LogCtx, LogRead, LogWrite, LogWriteOptions, LSN,
};

#[derive(Clone)]
pub struct FileLogOptions {
    pub dir: PathBuf,
    pub sync_policy: FileSyncPolicy,
    pub log_size: u64,
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
    NoEnoughSpace(u64), // No enouph space in log file. Caller could switch or enlarge file.
}

// Write a record to writer.
fn write_record<Op: Serialize>(
    mut writer: impl Write,
    space_remain: u64,
    op: Op,
) -> Result<WriteRecordStatus> {
    let payload_buf = bincode::serialize(&op)?;
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

struct SingleFilePendingState {
    writer: BufWriter<File>,
    file: File,
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
    cur_written: u64,
    cur_file: SingleFilePendingState,
    prev_file: Option<SingleFilePendingState>,
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
    fn open_log(start_lsn: LSN, options: &FileLogOptions) -> Result<File> {
        let path = make_file_name(start_lsn);
        let file = OpenOptions::new().append(true).truncate(true).open(path)?;
        file.set_len(options.log_size)?;
        Ok(file)
    }

    fn iter_file_pending(
        state: &WriteSyncState,
    ) -> impl '_ + Iterator<Item = &SingleFilePendingState> {
        state
            .prev_file
            .iter()
            .chain(std::iter::once(&state.cur_file))
    }

    fn iter_file_pending_mut(
        state: &mut WriteSyncState,
    ) -> impl '_ + Iterator<Item = &mut SingleFilePendingState> {
        state
            .prev_file
            .iter_mut()
            .chain(std::iter::once(&mut state.cur_file))
    }

    // Wait for kill or periodic sync.
    fn wait_for_kill_or_periodic_sync<'a>(
        sync: &WriteSyncShared,
        mut state: MutexGuard<'a, WriteSyncState>,
    ) -> MutexGuard<'a, WriteSyncState> {
        while !sync.killed.load(Ordering::Relaxed) {
            let next_sync_time = state.next_sync_time.expect("set during thread spawn");
            let now = Instant::now();

            // Any pending write?
            if Self::iter_file_pending(&*state).any(|f| f.pending_lsn > f.sync_lsn) {
                // We have pending writes. Sync them when:
                // 1. Asked by the write options (i.e. force_sync flag).
                // 2. Asked by the periodical sync policy.
                let case_sync_time = || now >= next_sync_time;
                let case_force_sync =
                    || Self::iter_file_pending(&*state).any(|f| f.request_force_sync);
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
        let mut prev_lsn = 0;
        let mut file_lsn_tups = vec![];
        for p in Self::iter_file_pending_mut(&mut *state) {
            p.request_force_sync = false;
            p.sync_lsn = p.pending_lsn;
            p.writer.flush()?;
            file_lsn_tups.push((p.file.try_clone()?, p.pending_lsn));

            // Sanity check file iterate order. Old log file must comes first.
            assert!(p.pending_lsn >= prev_lsn);
            prev_lsn = p.pending_lsn;
        }

        // Forget about previous log file if exists. Log switching is done.
        state.prev_file.take();

        // Unlock before syncing.
        drop(state);

        // No race here, because we are in the only sync thread.
        for (f, lsn) in file_lsn_tups {
            f.sync_all()?;
            (sync.sink)(lsn);
        }

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
                        state = Self::wait_for_kill_or_periodic_sync(&*sync, state);

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
        let file = Self::open_log(start_lsn, &options)?;
        let writer = BufWriter::new(file.try_clone()?);

        let state = WriteSyncState {
            next_sync_time: None,
            prev_file: None,
            cur_file: SingleFilePendingState {
                file,
                writer,
                size: options.log_size,
                pending_lsn: 0,
                sync_lsn: 0,
                request_force_sync: false,
            },
            cur_written: 0,
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
            let space_remain = self.sync_shared.options.log_size - state.cur_written;
            let status = write_record(&mut state.cur_file.writer, space_remain, op)?;
            match status {
                WriteRecordStatus::Done(written) => {
                    state.cur_written += written;
                    state.cur_file.pending_lsn = lsn;
                    state.cur_file.request_force_sync |= options.force_sync;
                    break;
                }
                WriteRecordStatus::NoEnoughSpace(space) => {
                    ensure!(
                        space <= self.sync_shared.options.log_size,
                        "op larger than log size is currently not supported"
                    );

                    // Switch file.

                    while state.prev_file.is_some() {
                        // TODO: wait prev file sync
                    }

                    assert!(state.prev_file.is_none());
                    let file = Self::open_log(lsn, &self.sync_shared.options)?;
                    let new_file = SingleFilePendingState {
                        writer: BufWriter::new(file.try_clone()?),
                        file,
                        pending_lsn: 0,
                        sync_lsn: 0,
                        size: self.sync_shared.options.log_size,
                        request_force_sync: false,
                    };

                    let old_cur_file = std::mem::replace(&mut state.cur_file, new_file);
                    state.prev_file = Some(old_cur_file);
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

    fn fire_write(&mut self, op: &Op, lsn: LSN, options: &LogWriteOptions) {
        self.inner
            .get_mut()
            .expect("not init")
            .fire_write(op, lsn, options);
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
            write: todo!(),
            discard: todo!(),
        }
    }
}
