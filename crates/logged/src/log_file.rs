use std::{
    cmp::Ord,
    fs::{read_dir, OpenOptions},
    hash,
    io::{BufReader, Cursor, Read},
    marker::PhantomData,
    path::{Path, PathBuf},
    ptr::hash,
    todo,
};

use anyhow::{anyhow, ensure, Result};
use byteorder::{LittleEndian, ReadBytesExt};
use crc32fast::Hasher;
use serde::{de::DeserializeOwned, Serialize};

use crate::{LogCtx, LogRead, LSN};

pub struct FileLogStorage<Op> {
    _op: PhantomData<Op>,
}

// Physical Representation of LogEntry:
// Magic 0xef: u8
// Checksum (of later fields): u32
// Type: u8
// Payload length: u32
// Payload: [u8]
struct LogEntry<Op> {
    lsn: LSN,
    op: Op,
}

const MAGIC: u8 = 0xef;

#[repr(u8)]
enum LogEntryType {
    Normal = 0,
}

struct Crc32Read<R> {
    inner: R,
    hasher: crc32fast::Hasher,
}

impl<R> Crc32Read<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            hasher: crc32fast::Hasher::new(),
        }
    }

    pub fn finalize(self) -> u32 {
        self.hasher.finalize()
    }
}

impl<R: Read> Read for Crc32Read<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.hasher.update(&buf[..n]);
        Ok(n)
    }
}

// Read entry never fails. Corrupted entry & all entries after it are discarded.
fn read_entry<Op>(mut reader: impl Read) -> Option<LogEntry<Op>> {
    let result = || -> Result<()> {
        let magic = reader.read_u8()?;
        ensure!(magic == MAGIC, "magic mismatch");

        let crc = reader.read_u32::<LittleEndian>()?;

        // Following reads would be covered by CRC.
        let mut reader_crc = Crc32Read::new(&mut reader);
        let typ = reader_crc.read_u8()?;
        let payload_len = reader_crc.read_u32::<LittleEndian>()?;
        let mut payload_buf = vec![0 as u8; payload_len as usize];
        reader_crc.read_exact(&mut payload_buf)?;

        // Compare CRC value.
        let crc_actual = reader_crc.finalize();
        ensure!(crc_actual == crc, "crc mismatch");

        Ok(())
    }();

    todo!()
}

fn write_entry<Op>(write: impl std::io::Write, entry: LogEntry<Op>) -> Result<()> {
    todo!()
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
    _op: PhantomData<Op>,
}

impl<R, Op> Iterator for ReadLogIter<R, Op>
where
    R: Read,
    Op: DeserializeOwned,
{
    type Item = (Op, LSN);

    fn next(&mut self) -> Option<Self::Item> {
        let entry = read_entry(&mut self.reader)?;
        Some((entry.op, entry.lsn))
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

impl<Op> LogCtx<Op>
where
    Op: Send + Sync + Clone + Serialize + DeserializeOwned + 'static,
{
    pub fn file(dir: &Path) -> Self {
        Self {
            read: Box::new(ReadImpl {
                dir: PathBuf::from(dir),
                _op: PhantomData::default(),
            }),
            write: todo!(),
            discard: todo!(),
        }
    }
}
