use std::{
    cmp::Ord,
    fs::{read_dir, OpenOptions},
    io::BufReader,
    marker::PhantomData,
    ops::RangeInclusive,
    path::{Path, PathBuf},
    todo,
};

use anyhow::{anyhow, ensure, Result};
use itertools::Itertools;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{LogCtx, LogRead, LSN};

pub struct FileLogStorage<Op> {
    _op: PhantomData<Op>,
}

#[derive(Serialize, Deserialize)]
struct LogEntry<Op> {
    lsn: LSN,
    op: Op,
}

// Read entry never fails. Corrupted entry & all entries after it are discarded.
fn read_entry<Op>(read: impl std::io::Read) -> Option<LogEntry<Op>> {
    todo!()
}

fn write_entry<Op>(write: impl std::io::Write, entry: LogEntry<Op>) -> Result<()> {
    todo!()
}

// Log File Name Format
// log-{start}-{end}

fn parse_file_name(name: &str) -> Result<RangeInclusive<LSN>> {
    let fields = name.split("-").collect::<Vec<_>>();
    ensure!(
        fields.len() == 3 && fields[0] == "log",
        "invalid log filename"
    );

    Ok(RangeInclusive::new(fields[1].parse()?, fields[2].parse()?))
}

fn make_file_name(range: RangeInclusive<LSN>) -> String {
    format!("log-{}-{}", range.start(), range.end())
}

struct ReadLogIter<R, Op> {
    reader: R,
    _op: PhantomData<Op>,
}

impl<R, Op> Iterator for ReadLogIter<R, Op>
where
    R: std::io::Read,
    Op: DeserializeOwned,
{
    type Item = (Op, LSN);

    fn next(&mut self) -> Option<Self::Item> {
        let entry = read_entry(&mut self.reader)?;
        Some((entry.op, entry.lsn))
    }
}

struct FileInfo {
    lsn_range: RangeInclusive<LSN>,
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

struct LogIter<Op> {
    logs: Vec<FileInfo>,
    _op: PhantomData<Op>,
}

impl<Op> Iterator for LogIter<Op> {
    type Item = (Op, LSN);

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
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
            let range = parse_file_name(name)?;
            if *range.end() >= start {
                files.push(FileInfo {
                    lsn_range: range,
                    file: OpenOptions::new().read(true).open(ent.path())?,
                });
            }
        }

        // Sort them by ascending order.
        files.sort_by(|a, b| Ord::cmp(&a.lsn_range.start(), &b.lsn_range.start()));

        // Ensure consecutive LSN among log files.
        let consecutive = files
            .iter()
            .tuple_windows()
            .all(|(cur, next)| *cur.lsn_range.end() + 1 == *next.lsn_range.start());
        ensure!(consecutive, "log is not consecutive");

        // Iterate over files, flat map logs.
        Ok(Box::new(files.into_iter().flat_map(|f| f.iter_logs())))
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
