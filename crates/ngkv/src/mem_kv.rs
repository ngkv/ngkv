use std::{
    collections::{btree_map::Entry, BTreeMap},
    ops::{Deref, RangeBounds},
    sync::{Arc, Mutex},
};

use crate::{
    CompareAndSwapStatus, CowArc, Kv, Kvp, RangeIterator, ReadOptions, Result, Snapshot,
    WriteOptions,
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

/// Downcast snapshot into impl.
fn snap_impl(s: &dyn Snapshot) -> &SnapshotImpl {
    unsafe { &*(s as *const dyn Snapshot as *const SnapshotImpl) }
}

struct RangeIterImpl {
    /// Guarantees `iter` is valid.
    _tree: CowArc<BTreeMap<Vec<u8>, Vec<u8>>>,

    /// Lifetime is not really static here, but is the same as `_tree`.
    iter: Box<dyn DoubleEndedIterator<Item = (&'static Vec<u8>, &'static Vec<u8>)>>,
}

fn map_item(i: Option<(&Vec<u8>, &Vec<u8>)>) -> Option<Result<Kvp>> {
    i.map(|(k, v)| {
        Ok(Kvp {
            key: k.to_owned(),
            value: v.to_owned(),
        })
    })
}

impl Iterator for RangeIterImpl {
    type Item = Result<Kvp>;

    fn next(&mut self) -> Option<Self::Item> {
        map_item(self.iter.next())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }

    fn last(self) -> Option<Self::Item>
    where
        Self: Sized,
    {
        map_item(self.iter.last())
    }
}

impl DoubleEndedIterator for RangeIterImpl {
    fn next_back(&mut self) -> Option<Self::Item> {
        map_item(self.iter.next_back())
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
    fn put(&self, _options: &WriteOptions, key: &[u8], value: &[u8]) -> Result<()> {
        let mut state = self.sh_mem.state.lock().unwrap();
        state.tree.insert(key.to_owned(), value.to_owned());
        Ok(())
    }

    fn delete(&self, _options: &WriteOptions, key: &[u8]) -> Result<()> {
        let mut state = self.sh_mem.state.lock().unwrap();
        state.tree.remove(key);
        Ok(())
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
        range: impl RangeBounds<Vec<u8>>,
    ) -> Result<Box<dyn '_ + RangeIterator>> {
        let tree = if let Some(snapshot) = options.snapshot.map(snap_impl) {
            snapshot.tree.clone()
        } else {
            let state = self.sh_mem.state.lock().unwrap();
            state.tree.clone()
        };

        // SAFETY: Tree outlives iterator, and tree.deref() never changes
        // because of readonly access.
        let tree_ref: &'static BTreeMap<Vec<u8>, Vec<u8>> = unsafe { &*(tree.deref() as *const _) };

        Ok(Box::new(RangeIterImpl {
            _tree: tree,
            iter: Box::new(tree_ref.range(range)),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mem_kv_put_get() {
        let value = "Hello world!";
        let kv = MemKv::new();

        kv.put(
            &WriteOptions::default(),
            "test".as_bytes(),
            value.as_bytes(),
        )
        .unwrap();

        let get_res = kv
            .get(&ReadOptions::default(), "test".as_bytes())
            .unwrap()
            .unwrap();
        assert_eq!(get_res, value.as_bytes());
    }

    #[test]
    fn mem_kv_snapshot_read() {
        let value = "Hello world!";
        let kv = MemKv::new();

        kv.put(
            &WriteOptions::default(),
            "test".as_bytes(),
            value.as_bytes(),
        )
        .unwrap();

        let snap = kv.snapshot();

        kv.delete(&WriteOptions::default(), "test".as_bytes())
            .unwrap();

        // Test non-snapshot read.
        assert!(kv
            .get(&ReadOptions::default(), "test".as_bytes())
            .unwrap()
            .is_none());

        // Test snapshot read.
        let get_res = kv
            .get(
                &ReadOptions {
                    snapshot: Some(&*snap),
                },
                "test".as_bytes(),
            )
            .unwrap()
            .unwrap();
        assert_eq!(get_res, value.as_bytes());
    }

    // TODO: test range read.
}
