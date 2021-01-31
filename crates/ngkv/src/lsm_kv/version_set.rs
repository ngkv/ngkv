use crate::{file_log_options, key_comparator, Error, Result, ShouldRun, Task, TaskCtl};
use bincode::Options;
use itertools::Itertools;
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use std::{
    cmp::max,
    collections::{HashMap, HashSet},
    fs,
    io::Write,
    ops::Deref,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use wal_fsm::{Fsm, FsmOp, Lsn};

pub const LEVEL_COUNT: u32 = 10;

fn bincode_options() -> impl bincode::Options {
    bincode::DefaultOptions::new()
        .with_little_endian()
        .with_varint_encoding()
}

fn current_file_path(dir: &Path) -> PathBuf {
    PathBuf::from(dir).join("current")
}

fn temp_file_path(dir: &Path) -> PathBuf {
    PathBuf::from(dir).join(format!("temp-{}", uuid::Uuid::new_v4().to_simple()))
}

fn log_dir(parent_dir: &Path) -> Result<PathBuf> {
    let dir = PathBuf::from(parent_dir).join("wal");
    if !dir.is_dir() {
        fs::create_dir(&dir)?;
    }
    Ok(dir)
}

#[derive(Serialize, Deserialize, Clone)]
struct SstMetaRaw {
    id: u32,
    level: u32,
    data_lsn_lo: Lsn,
    data_lsn_hi: Lsn,
    key_lo: Vec<u8>,
    key_hi: Vec<u8>,
}

struct SstMeta {
    /// Box is used to keep the memory location of `SstMetaRaw` stable, since it
    /// would be aliased by raw pointers in `HandleViewCache`.
    raw: Box<SstMetaRaw>,

    // NOTE: Atomic is only used to provide convienient interior mutability. It
    // is actually protected by a mutex.
    rc: AtomicU32,
}

struct Version {
    id: u32,
    ssts: Vec<u32>,
    view_cache: OnceCell<Arc<HandleViewCache>>,

    // NOTE: Atomic is only used to provide convienient interior mutability. It
    // is actually protected by a mutex.
    rc: AtomicU32,
}

#[derive(Serialize, Deserialize, Clone)]
struct Checkpoint {
    version_lsn: wal_fsm::Lsn,
    ssts: Vec<SstMetaRaw>,
    version_id: u32,
}

#[derive(Serialize, Deserialize, Clone)]
struct VersionOp {
    sst_add: Vec<SstMetaRaw>,
    sst_del: Vec<u32>,
}

impl FsmOp for VersionOp {
    fn serialize(&self) -> wal_fsm::Result<Vec<u8>> {
        Ok(bincode_options().serialize(&self).unwrap())
    }

    fn deserialize(buf: &[u8]) -> wal_fsm::Result<Self> {
        bincode_options()
            .deserialize(buf)
            .map_err(|_| wal_fsm::Error::Corrupted("deserialization failed".into()))
    }
}

struct VersionFsmState {
    next_sst_id: u32,
    version_id: u32,
    version_map: HashMap<u32, Version>,
    sst_map: HashMap<u32, SstMeta>,
    version_lsn: wal_fsm::Lsn,
}

impl VersionFsmState {
    fn version_add_first(&mut self, version_id: u32, ssts: Vec<u32>) {
        assert!(self.version_map.len() == 0);

        self.internal_version_add(version_id, ssts);

        self.version_rc_inc(version_id);
        self.version_id = version_id;
    }

    fn version_switch(&mut self, ssts: Vec<u32>) {
        assert!(self.version_map.len() > 0);

        let version_id = self.version_id + 1;
        self.internal_version_add(version_id, ssts);

        self.version_rc_inc(version_id);
        self.version_rc_dec(self.version_id);
        self.version_id = version_id;
    }

    fn internal_version_add(&mut self, version_id: u32, ssts: Vec<u32>) {
        let version = Version {
            id: version_id,
            ssts,
            rc: AtomicU32::new(0),
            view_cache: Default::default(),
        };
        for sst_id in version.ssts.iter() {
            self.sst_rc_inc(*sst_id);
        }
        self.version_map.insert(version.id, version);
    }

    fn version_rc_inc(&mut self, version_id: u32) {
        let version = self.version_map.get_mut(&version_id).unwrap();
        version.rc.fetch_add(1, Ordering::Relaxed);
    }

    fn version_rc_dec(&mut self, version_id: u32) {
        let version = self.version_map.get_mut(&version_id).unwrap();
        let prev = version.rc.fetch_sub(1, Ordering::Relaxed);
        if prev == 1 {
            let version = self.version_map.remove(&version_id).unwrap();
            for sst in version.ssts {
                self.sst_rc_dec(sst);
            }
        }
    }

    fn sst_add(&mut self, sst_raw: SstMetaRaw) {
        // NOTE: The update of next SST id is only possible during recovery.
        self.next_sst_id = max(self.next_sst_id, sst_raw.id + 1);
        self.sst_map.insert(
            sst_raw.id,
            SstMeta {
                raw: Box::new(sst_raw),
                rc: AtomicU32::new(0),
            },
        );
    }

    fn sst_rc_inc(&mut self, sst_id: u32) {
        let sst = self.sst_map.get_mut(&sst_id).unwrap();
        sst.rc.fetch_add(1, Ordering::Relaxed);
    }

    fn sst_rc_dec(&mut self, sst_id: u32) {
        let sst = self.sst_map.get_mut(&sst_id).unwrap();
        let prev = sst.rc.fetch_sub(1, Ordering::Relaxed);
        if prev == 1 {
            let _sst = self.sst_map.remove(&sst_id).unwrap();
            // TODO: remove file
        }
    }

    fn make_view_cache(&self, version_id: u32) -> HandleViewCache {
        let version = self.version_map.get(&version_id).unwrap();

        let sst_raw_vec = version
            .ssts
            .iter()
            .map(|sst_id| self.sst_map.get(sst_id).unwrap().raw.deref())
            .collect_vec();

        let mut level_info_vec = vec![];
        for _ in 0..LEVEL_COUNT {
            level_info_vec.push(LevelInfo { ssts: vec![] });
        }

        for sst_raw in sst_raw_vec {
            level_info_vec[sst_raw.level as usize].ssts.push(SstInfo {
                raw: sst_raw as *const SstMetaRaw,
            });
        }

        for _ in 0..LEVEL_COUNT {
            level_info_vec[0]
                .ssts
                .sort_by(key_comparator(|sst: &SstInfo| sst.key_range().0));
        }

        HandleViewCache {
            levels: level_info_vec,
        }
    }

    /// Recover from checkpoint.
    fn recover_checkpoint(&mut self, ckpt: Checkpoint) {
        self.version_lsn = ckpt.version_lsn;

        // Recover SSTs.
        for sst_raw in ckpt.ssts.iter() {
            self.sst_add(sst_raw.clone());
        }

        // Recover current version.
        self.version_add_first(
            ckpt.version_id,
            ckpt.ssts.iter().map(|raw| raw.id).collect_vec(),
        );

        // Increase the ref count of current version.
        let cur_version = self.version_id;
        self.version_rc_inc(cur_version);
    }

    fn checkpoint(&self) -> Checkpoint {
        Checkpoint {
            version_lsn: self.version_lsn,
            ssts: self
                .sst_map
                .values()
                .map(|sst| sst.raw.deref().clone())
                .collect_vec(),
            version_id: self.version_id,
        }
    }
}

struct VersionFsmShared {
    dir: PathBuf,
    state: Mutex<VersionFsmState>,
}

struct CheckpointTask {
    fsm_shared: Arc<VersionFsmShared>,
    sink: Box<dyn wal_fsm::ReportSink>,
}

impl Task for CheckpointTask {
    type Error = Error;

    fn should_run(&mut self) -> ShouldRun {
        // TODO: check log size, return ShouldRun::Yes if necessary.
        ShouldRun::AskMeLater(Duration::from_secs(10))
    }

    fn run(&mut self) -> Result<(), Self::Error> {
        let state = self.fsm_shared.state.lock().unwrap();
        let version_lsn = state.version_lsn;
        let ckpt = state.checkpoint();
        drop(state);

        let buf = bincode_options().serialize(&ckpt).unwrap();

        // Write checkpoint into temp file.
        let temp_file_path = temp_file_path(&self.fsm_shared.dir);
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_file_path)?;
        file.write_all(&buf)?;
        file.sync_all()?;

        // Atomically set current version file.
        let cur_file_path = current_file_path(&self.fsm_shared.dir);
        fs::rename(&temp_file_path, &cur_file_path)?;

        self.sink.report_checkpoint_lsn(version_lsn);

        Ok(())
    }
}

struct VersionFsm {
    ckpt_ctl: OnceCell<TaskCtl<CheckpointTask>>,
    shared: Arc<VersionFsmShared>,
}

impl VersionFsm {}

impl Fsm for VersionFsm {
    type E = Error;
    type Op = VersionOp;

    fn init(&self, sink: Box<dyn wal_fsm::ReportSink>) -> Result<wal_fsm::Init, Self::E> {
        let mut state = self.shared.state.lock().unwrap();

        let cur_file_path = current_file_path(&self.shared.dir);
        if cur_file_path.is_file() {
            // Recover from checkpoint.
            let buf = fs::read(&cur_file_path)?;
            let ckpt: Checkpoint = bincode_options().deserialize(&buf).unwrap();
            state.recover_checkpoint(ckpt);
        } else {
            // Add default version.
            state.version_add_first(0, vec![]);
        }

        self.ckpt_ctl
            .set(TaskCtl::new(CheckpointTask {
                fsm_shared: self.shared.clone(),
                sink,
            }))
            .map_err(|_| ())
            .expect("already init");

        Ok(wal_fsm::Init {
            next_lsn: state.version_lsn + 1,
        })
    }

    fn apply(&self, op: Self::Op, lsn: wal_fsm::Lsn) -> Result<(), Self::E> {
        let mut state = self.shared.state.lock().unwrap();

        // Set of SST id refered by the next version.
        let mut sst_ids = HashSet::<u32>::new();

        // Copy SST id from latest version.
        sst_ids.extend(
            state
                .version_map
                .get(&state.version_id)
                .unwrap()
                .ssts
                .iter(),
        );

        // Perform SST set add & remove.
        for sst_raw in op.sst_add {
            assert!(sst_ids.insert(sst_raw.id));
            // Insert SST meta.
            state.sst_add(sst_raw);
        }
        for id in op.sst_del {
            assert!(sst_ids.remove(&id));
        }

        // Switch current version.
        state.version_switch(sst_ids.into_iter().collect_vec());

        assert_eq!(state.version_lsn + 1, lsn);
        state.version_lsn = lsn;

        Ok(())
    }
}

pub struct SstInfo {
    raw: *const SstMetaRaw,
}

unsafe impl Send for SstInfo {}
unsafe impl Sync for SstInfo {}

impl SstInfo {
    fn get_raw(&self) -> &SstMetaRaw {
        // SAFETY: *raw must be living, since the version handle keeps the ref
        // count of version & SST non-zero.
        unsafe { &*self.raw }
    }

    pub fn key_range(&self) -> (&[u8], &[u8]) {
        let raw = self.get_raw();
        (&*raw.key_lo, &*raw.key_hi)
    }

    pub fn lsn_range(&self) -> (wal_fsm::Lsn, wal_fsm::Lsn) {
        let raw = self.get_raw();
        (raw.data_lsn_lo, raw.data_lsn_hi)
    }

    pub fn register_seek(&self) {
        // TODO: register a seek operation, to help pick compaction.
    }
}

struct HandleViewCache {
    levels: Vec<LevelInfo>,
}

pub struct LevelInfo {
    ssts: Vec<SstInfo>,
}

impl LevelInfo {
    pub fn ssts(&self) -> &[SstInfo] {
        &*self.ssts
    }
}

pub struct VersionHandle<'a> {
    fsm: &'a VersionFsm,
    version_id: u32,
    view_cache: Arc<HandleViewCache>,
}

impl VersionHandle<'_> {
    pub fn levels(&self) -> &[LevelInfo] {
        &*self.view_cache.levels
    }
}

impl Drop for VersionHandle<'_> {
    fn drop(&mut self) {
        let mut state = self.fsm.shared.state.lock().unwrap();
        state.version_rc_dec(self.version_id);
    }
}

pub struct Compaction {
    // TODO: compaction info
}

pub struct VersionEditBuilder {
    sst_add: Vec<SstMetaRaw>,
    sst_del: Vec<u32>,
}

impl VersionEditBuilder {
    pub fn new() -> Self {
        Self {
            sst_add: vec![],
            sst_del: vec![],
        }
    }

    pub fn add_sst(
        mut self,
        id: u32,
        level: u32,
        lsn_range: (Lsn, Lsn),
        key_range: (Vec<u8>, Vec<u8>),
    ) -> Self {
        self.sst_add.push(SstMetaRaw {
            id,
            level,
            data_lsn_lo: lsn_range.0,
            data_lsn_hi: lsn_range.1,
            key_lo: key_range.0,
            key_hi: key_range.1,
        });
        self
    }

    pub fn del_sst(mut self, id: u32) -> Self {
        self.sst_del.push(id);
        self
    }

    pub fn build(self) -> VersionEdit {
        VersionEdit {
            op: VersionOp {
                sst_add: self.sst_add.clone(),
                sst_del: self.sst_del,
            },
        }
    }
}

pub struct VersionEdit {
    op: VersionOp,
}

pub struct VersionSet {
    fsm: wal_fsm::WalFsm<VersionFsm>,
}

impl VersionSet {
    pub fn new(dir: &Path) -> Result<Self> {
        Ok(Self {
            fsm: wal_fsm::WalFsm::new(
                VersionFsm {
                    ckpt_ctl: Default::default(),
                    shared: Arc::new(VersionFsmShared {
                        dir: PathBuf::from(dir),
                        state: Mutex::new(VersionFsmState {
                            sst_map: Default::default(),
                            next_sst_id: 0,
                            version_id: 0,
                            version_map: Default::default(),
                            version_lsn: 0,
                        }),
                    }),
                },
                wal_fsm::LogCtx::file(&file_log_options(log_dir(dir)?)),
            )?,
        })
    }

    /// Garbage collect unused files.
    pub fn gc(&self) -> Result<()> {
        // TODO: garbage collect
        Ok(())
    }

    /// Get current version handle.
    pub fn current(&self) -> VersionHandle<'_> {
        let mut state = self.fsm.shared.state.lock().unwrap();
        let version_id = state.version_id;
        state.version_rc_inc(version_id);

        let version = state.version_map.get(&version_id).unwrap();

        // Get or initialize view cache.
        let view_cache = version
            .view_cache
            .get_or_init(|| Arc::new(state.make_view_cache(version_id)));

        VersionHandle {
            fsm: &self.fsm,
            version_id,
            view_cache: view_cache.clone(),
        }
    }

    /// Allocate a new SST id.
    pub fn new_sst_id(&self) -> u32 {
        // NOTE: We have restored proper next SST id during recovery (in
        // `sst_add`), and this method call happens only after recovery. Thus,
        // it is not possible to return an existing SST id.
        let mut state = self.fsm.shared.state.lock().unwrap();
        let cur = state.next_sst_id;
        state.next_sst_id += 1;
        cur
    }

    /// Log and durably apply a version edit.
    pub fn log_and_apply(&self, edit: VersionEdit) -> Result<()> {
        self.fsm
            .apply(edit.op, wal_fsm::ApplyOptions { is_sync: true })?;
        Ok(())
    }

    /// Get all live SST id in ascending order.
    pub fn live_sst(&self) -> Vec<u32> {
        let state = self.fsm.shared.state.lock().unwrap();
        let mut res = state.sst_map.keys().cloned().collect_vec();
        res.sort();
        res
    }

    // TODO: pick compaction
    pub fn pick_compaction(&self) -> Option<Compaction> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;

    fn temp_dir() -> Result<TempDir> {
        let dir = TempDir::new("wal_fsm")?;
        Ok(dir)
    }

    #[test]
    fn version_set_simple() {
        let temp_dir = temp_dir().unwrap();

        let sst_id;

        let lsn_range = (100, 200);
        let key_range = (vec![10], vec![20]);

        {
            let vs = VersionSet::new(temp_dir.path()).unwrap();
            sst_id = vs.new_sst_id();
            let edit = VersionEditBuilder::new()
                .add_sst(sst_id, 0, lsn_range, key_range.clone())
                .build();
            vs.log_and_apply(edit).unwrap();
        }

        {
            let vs = VersionSet::new(temp_dir.path()).unwrap();
            let version = vs.current();
            let levels = version.levels();

            let l0_ssts = levels[0].ssts();
            assert_eq!(l0_ssts.len(), 1);

            let s0 = &l0_ssts[0];
            assert_eq!(s0.lsn_range(), lsn_range);
            assert_eq!(s0.key_range(), (&*key_range.0, &*key_range.1));
        }
    }

    #[test]
    fn version_set_add_del_check_live() {
        let temp_dir = temp_dir().unwrap();

        let sst_id1;
        let sst_id2;

        let vs = VersionSet::new(temp_dir.path()).unwrap();

        {
            sst_id1 = vs.new_sst_id();
            let lsn_range = (100, 200);
            let key_range = (vec![10], vec![20]);
            let edit = VersionEditBuilder::new()
                .add_sst(sst_id1, 0, lsn_range, key_range.clone())
                .build();
            vs.log_and_apply(edit).unwrap();
        }

        let version = vs.current();

        {
            sst_id2 = vs.new_sst_id();
            let lsn_range = (201, 300);
            let key_range = (vec![10], vec![20]);
            let edit = VersionEditBuilder::new()
                .add_sst(sst_id2, 0, lsn_range, key_range.clone())
                .del_sst(sst_id1)
                .build();
            vs.log_and_apply(edit).unwrap();
        }

        assert_eq!(vs.live_sst(), vec![sst_id1, sst_id2]);

        drop(version);

        assert_eq!(vs.live_sst(), vec![sst_id2]);
    }
}
