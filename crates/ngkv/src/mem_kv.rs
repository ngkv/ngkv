use std::{
    any::Any,
    borrow::Cow,
    collections::{btree_map::Entry, BTreeMap},
    marker::PhantomData,
    sync::{Arc, Mutex},
    todo,
};

use crate::{
    CompareAndSwapStatus, CowArc, Kv, Kvp, RangeBound, RangeIterator, ReadOptions, Result,
    Snapshot, WriteOptions,
};

struct MemState {
    tree: CowArc<BTreeMap<Vec<u8>, Vec<u8>>>,
}

struct MemShared {
    state: Mutex<MemState>,
}

pub struct MemKv {
    sh_mem: Arc<MemShared>,
}

struct SnapshotImpl {
    tree: CowArc<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl SnapshotImpl {}

impl Snapshot for SnapshotImpl {}

fn snap_impl(s: &dyn Snapshot) -> &SnapshotImpl {
    unsafe { &*(s as *const dyn Snapshot as *const SnapshotImpl) }
}

struct RangeIterImpl {
    tree: CowArc<BTreeMap<Vec<u8>, Vec<u8>>>,
    range: RangeBound,
    cur_key: Option<Vec<u8>>, // last valid key returned by iterator
}

impl Iterator for RangeIterImpl {
    type Item = Kvp;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl RangeIterator for RangeIterImpl {}

impl MemKv {
    pub fn new() -> Self {
        Self {
            sh_mem: Arc::new(MemShared {
                state: Mutex::new(MemState {
                    tree: CowArc::new(BTreeMap::new()),
                }),
            }),
        }
    }
}

impl Kv for MemKv {
    fn put(&self, _options: &WriteOptions, key: &[u8], value: &[u8]) {
        let mut state = self.sh_mem.state.lock().unwrap();
        state.tree.insert(key.to_owned(), value.to_owned());
    }

    fn delete(&self, _options: &WriteOptions, key: &[u8]) {
        let mut state = self.sh_mem.state.lock().unwrap();
        state.tree.remove(key);
    }

    fn compare_and_swap(
        &self,
        _options: &WriteOptions,
        key: &[u8],
        cur_value: Option<&[u8]>,
        new_value: Option<&[u8]>,
    ) -> Result<CompareAndSwapStatus> {
        let mut state = self.sh_mem.state.lock().unwrap();
        let entry = state.tree.entry(key.to_owned());
        Ok(match (entry, cur_value) {
            // Actual value present, match.
            (Entry::Occupied(mut entry), Some(cur_value)) if &entry.get()[..] == cur_value => {
                if let Some(new_value) = new_value {
                    *entry.get_mut() = new_value.to_owned();
                } else {
                    entry.remove_entry();
                }
                CompareAndSwapStatus::Succeeded
            }

            // Actual value present, not match.
            (Entry::Occupied(entry), _) => CompareAndSwapStatus::CurrentMismatch {
                cur_value: Some(entry.get().clone()),
            },

            // Actual value not present, match.
            (Entry::Vacant(entry), None) => {
                if let Some(new_value) = new_value {
                    entry.insert(new_value.to_owned());
                }
                CompareAndSwapStatus::Succeeded
            }

            // Actual value not present, not match.
            (Entry::Vacant(_), _) => CompareAndSwapStatus::CurrentMismatch { cur_value: None },
        })
    }

    fn snapshot(&self) -> Box<dyn '_ + Snapshot> {
        let state = self.sh_mem.state.lock().unwrap();
        Box::new(SnapshotImpl {
            tree: state.tree.clone(),
        })
    }

    fn get(&self, options: &ReadOptions<'_>, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let v = if let Some(snapshot) = options.snapshot.map(snap_impl) {
            snapshot.tree.get(key).cloned()
        } else {
            let state = self.sh_mem.state.lock().unwrap();
            state.tree.get(key).cloned()
        };

        Ok(v)
    }

    fn range(
        &self,
        options: &ReadOptions<'_>,
        range: RangeBound,
    ) -> Result<Box<dyn RangeIterator>> {
        let tree = if let Some(snapshot) = options.snapshot.map(snap_impl) {
            snapshot.tree.clone()
        } else {
            let state = self.sh_mem.state.lock().unwrap();
            state.tree.clone()
        };

        Ok(Box::new(RangeIterImpl {
            cur_key: None,
            range,
            tree,
        }))
    }
}
