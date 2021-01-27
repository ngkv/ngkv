mod sample_fsm;

use super::*;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crossbeam::channel::Receiver;

use std::{
    io::Write,
    time::{Duration, Instant},
};

const ENABLE_CHECK_NON_BLOCKING: bool = false;
const NON_BLOCKING_THRESHOLD: Duration = Duration::from_millis(1);

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

pub fn check_non_blocking(f: impl FnOnce()) {
    let now = Instant::now();
    f();
    let time = Instant::now() - now;
    if ENABLE_CHECK_NON_BLOCKING {
        assert!(
            time < NON_BLOCKING_THRESHOLD,
            "check_non_blocking failed ({}ms)",
            time.as_millis()
        );
    }
}

pub fn check_blocking(f: impl FnOnce(), dur: Duration) {
    let now = Instant::now();
    f();
    let time = Instant::now() - now;
    assert!(
        time >= dur,
        "check_blocking failed ({}ms)",
        time.as_millis()
    );
}

pub struct LogSyncWait {
    cur_lsn: Lsn,
    recv: Receiver<Lsn>,
}

impl LogSyncWait {
    pub fn create() -> (Self, Box<dyn Send + Sync + 'static + Fn(Lsn)>) {
        let (send, recv) = crossbeam::channel::unbounded();

        let sink = Box::new(move |lsn| {
            send.send(lsn).unwrap();
        });

        (Self { cur_lsn: 0, recv }, sink)
    }

    pub fn wait(&mut self, lsn: Lsn) {
        while lsn > self.cur_lsn {
            self.cur_lsn = self.recv.recv().unwrap();
        }
    }
}

// A log entry of TestOp is 18 bytes
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct TestOp {
    pub no: Lsn, // equal to Lsn
}

impl FsmOp for TestOp {
    fn serialize(&self) -> Vec<u8> {
        let mut buf = vec![];
        buf.write_u64::<LittleEndian>(self.no).unwrap();
        buf
    }

    fn deserialize(mut buf: &[u8]) -> Result<Self> {
        assert!(buf.len() == 8);
        let no = buf.read_u64::<LittleEndian>()?;
        Ok(TestOp { no })
    }
}

// Basic sanity checks for read result.
pub(crate) fn assert_test_op_iter<'a>(iter: impl Iterator<Item = &'a LogRecord<TestOp>>) {
    let mut prev_lsn = None;
    for rec in iter {
        // Check TestOp::no == Lsn.
        assert_eq!(rec.op.no, rec.lsn);

        // Check Lsn consecutive.
        if let Some(prev) = prev_lsn {
            assert_eq!(prev + 1, rec.lsn);
        }
        prev_lsn = Some(rec.lsn);
    }
}

pub(crate) fn test_op_write(w: &mut dyn LogWrite<TestOp>, lsn: Lsn, options: &LogWriteOptions) {
    w.fire_write(
        &LogRecord {
            op: TestOp { no: lsn },
            lsn,
        },
        options,
    )
    .unwrap()
}
