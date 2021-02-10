mod lsm_kv;
#[cfg(test)]
mod mem_kv;
mod task_ctl;

use std::{io, ops::RangeBounds, path::PathBuf, time::Duration};

pub use crate::{lsm_kv::LsmKv, task_ctl::*};

use thiserror::Error;

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub type Lsn = wal_fsm::Lsn;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("reportable bug: {0}")]
    ReportableBug(String),
    #[error("data corrupted: {0}")]
    Corrupted(String),
    #[error(transparent)]
    Wal(Box<wal_fsm::Error<Error>>),
}

impl From<wal_fsm::Error<Error>> for Error {
    fn from(e: wal_fsm::Error<Error>) -> Self {
        Error::Wal(Box::new(e))
    }
}

#[derive(Default)]
pub struct WriteOptions {
    pub is_sync: bool,
}

#[derive(Default)]
pub struct ReadOptions<'a> {
    snapshot: Option<&'a dyn Snapshot>,
}

pub enum CompareAndSwapStatus {
    Succeeded,
    CurrentMismatch { cur_value: Option<Vec<u8>> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Kvp {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

pub trait RangeIterator: DoubleEndedIterator<Item = Result<Kvp>> {}

pub trait Snapshot {}

pub trait Kv {
    fn put(&self, options: &WriteOptions, key: &[u8], value: &[u8]) -> Result<()>;

    fn delete(&self, options: &WriteOptions, key: &[u8]) -> Result<()>;

    fn compare_and_swap(
        &self,
        options: &WriteOptions,
        key: &[u8],
        cur_value: Option<&[u8]>,
        new_value: Option<&[u8]>,
    ) -> Result<CompareAndSwapStatus>;

    fn snapshot(&self) -> Box<dyn '_ + Snapshot>;

    fn get(&self, options: &ReadOptions<'_>, key: &[u8]) -> Result<Option<Vec<u8>>>;

    fn range(
        &self,
        options: &ReadOptions<'_>,
        range: impl RangeBounds<Vec<u8>>,
    ) -> Result<Box<dyn '_ + RangeIterator>>;
}


fn file_log_options(dir: PathBuf) -> wal_fsm::FileLogOptions {
    wal_fsm::FileLogOptions {
        dir,
        log_step_size: 1 << 20,    // 1MB
        log_switch_size: 10 << 20, // 10MB
        max_pending: 1024,
        sync_policy: wal_fsm::FileSyncPolicy::Periodical(Duration::from_secs(1)),
    }
}

/// Workaround of the `sort_by_key` lifetime issue.
fn key_comparator<T, K: Ord + ?Sized>(
    mut f: impl FnMut(&T) -> &K,
) -> impl FnMut(&T, &T) -> std::cmp::Ordering {
    move |a, b| f(a).cmp(f(b))
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    #[ctor::ctor]
    fn init_logger() {
        let _ = env_logger::builder()
            .format(|buf, record| {
                writeln!(
                    buf,
                    "{}:{} {} [{}] - {}",
                    record.file().unwrap_or("unknown"),
                    record.line().unwrap_or(0),
                    chrono::Local::now().format("%Y-%m-%dT%H:%M:%S"),
                    record.level(),
                    record.args()
                )
            })
            .is_test(true)
            .try_init();
    }
}
