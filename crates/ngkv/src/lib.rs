use wal_fsm::{FileLogOptions, Fsm, FsmOp, LogCtx};

mod varint;

struct KvFsm {}

#[derive(Clone)]
enum KvFsmOp {
    Put { Key: Vec<u8>, Value: Vec<u8> },
    Delete { Key: Vec<u8> },
}

impl FsmOp for KvFsmOp {
    fn serialize(&self) -> Vec<u8> {
        todo!()
    }

    fn deserialize(buf: &[u8]) -> wal_fsm::Result<Self> {
        todo!()
    }
}

impl Fsm for KvFsm {
    type Op = KvFsmOp;

    fn init(&self, sink: Box<dyn wal_fsm::ReportSink>) -> wal_fsm::Init {
        todo!()
    }

    fn apply(&self, op: Self::Op, lsn: wal_fsm::Lsn) {
        todo!()
    }
}
