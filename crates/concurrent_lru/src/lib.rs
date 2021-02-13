use std::{
    cell::UnsafeCell,
    collections::{hash_map::Entry, HashMap},
    hash::Hash,
    marker::PhantomData,
    ops::{Deref, DerefMut},
    ptr::{null, null_mut, NonNull},
    sync::{
        atomic::{AtomicU32, Ordering},
        Mutex, MutexGuard,
    },
    todo,
};

use once_cell::sync::OnceCell;

struct NodeState<K, V> {
    rc: u32,
    key: Option<K>,
    // in_lru_list: bool,
    prev: *const Node<K, V>,
    next: *const Node<K, V>,
}

/// Represents an element in cache. The node must be in one of following states:
/// 1. In-use: The node is pinned (referenced) by at least one `CacheHandle`,
///    thus cannot be evicted. In such state, `rc` > 0, `prev` and `next` are
///    not valid.
/// 2. In-LRU-list: The node is not pinned (referenced) by any `CacheHandle`,
///    facing potential eviction. In such state, `rc` == 0, `prev` and `next`
///    fields form a circular linked list, in LRU order.
///
/// NOTE: Never obtain a mutable reference to nodes (except during
/// initialization). Otherwise, concurrent reader from `CacheHandle` would
/// violate the aliasing rules.
struct Node<K, V> {
    state: UnsafeCell<NodeState<K, V>>,
    value: OnceCell<V>,
}

struct LruState<K, V> {
    map: HashMap<K, Box<Node<K, V>>>,

    capacity: usize,

    /// Dummy head node of the circular linked list. Nodes are in LRU order.
    list_dummy: Box<Node<K, V>>,

    // Size of the circular linked list. Dummy head node is excluded. While this
    // is larger than capacity, we perform the eviction.
    list_size: usize,
}

pub struct Lru<K, V> {
    state: Mutex<LruState<K, V>>,
}

pub struct CacheHandle<'a, K: Hash + Eq + Clone, V> {
    lru: &'a Lru<K, V>,
    node: *const Node<K, V>,
}

impl<'a, K: Hash + Eq + Clone, V> CacheHandle<'a, K, V> {
    pub fn value(&self) -> &V {
        unsafe { (*self.node).value.get().unwrap() }
    }
}

impl<'a, K: Hash + Eq + Clone, V> Drop for CacheHandle<'a, K, V> {
    fn drop(&mut self) {
        if let Ok(guard) = self.lru.state.lock() {
            Lru::unref(guard, self.node);
        }
    }
}

impl<K: Hash + Eq + Clone, V> Lru<K, V> {
    unsafe fn link(cur: *const Node<K, V>, next: *const Node<K, V>) {
        (*(*cur).state.get()).next = next;
        (*(*next).state.get()).prev = cur;
    }

    /// Append node to list tail (newest).
    unsafe fn list_append(dummy: *const Node<K, V>, node: *const Node<K, V>) {
        let prev = (*(*dummy).state.get()).prev;
        Self::link(node, dummy);
        Self::link(prev, node);
    }

    unsafe fn list_remove(node: *const Node<K, V>) {
        let node = &mut *(*node).state.get();
        Self::link(node.prev, node.next);
        node.prev = null();
        node.next = null();
    }

    pub fn new(capacity: usize) -> Self {
        let mut dummy = Box::new(Node {
            state: UnsafeCell::new(NodeState {
                prev: null(),
                next: null(),
                key: None,
                rc: 0,
            }),
            value: OnceCell::new(),
        });

        dummy.state.get_mut().prev = dummy.as_ref();
        dummy.state.get_mut().next = dummy.as_ref();

        Self {
            state: Mutex::new(LruState {
                map: Default::default(),
                capacity,
                list_size: 0,
                list_dummy: dummy,
            }),
        }
    }

    fn maybe_evict(mut guard: MutexGuard<LruState<K, V>>, evict_all: bool) {
        let mut evict_nodes = vec![];

        let this = guard.deref_mut();
        let target_size = if evict_all { 0 } else { this.capacity };

        unsafe {
            while this.list_size > target_size {
                // Only obtain a shared reference to the dummy node.
                let oldest_ptr = (*this.list_dummy.as_ref().state.get()).next;
                assert!(oldest_ptr != this.list_dummy.as_ref());

                // Remove node from LRU list.
                let oldest = &mut *(*oldest_ptr).state.get();
                assert!(oldest.rc == 0);
                this.list_size -= 1;
                Self::list_remove(oldest_ptr);

                // Remove node from hash map.
                let node = this.map.remove(oldest.key.as_ref().unwrap()).unwrap();
                evict_nodes.push(node);
            }
        }

        // IMPORTANT: Drop user value without lock held.
        drop(guard);
        drop(evict_nodes);
    }

    fn unref(mut guard: MutexGuard<LruState<K, V>>, node_ptr: *const Node<K, V>) {
        let this = guard.deref_mut();
        let node = unsafe { &mut *(*node_ptr).state.get() };
        node.rc.checked_sub(1).unwrap();
        if node.rc == 0 {
            unsafe {
                Self::list_append(this.list_dummy.as_ref(), node_ptr);
            }
            Self::maybe_evict(guard, false);
        }
    }

    pub fn get_or_init<'a>(&'a self, key: K, init: impl FnOnce(&K) -> V) -> CacheHandle<'a, K, V> {
        let mut guard = self.state.lock().unwrap();
        let this = guard.deref_mut();
        let node_ptr = match this.map.entry(key.clone()) {
            Entry::Occupied(ent) => unsafe {
                let n = ent.get().deref();
                let node_ptr = n as *const Node<K, V>;
                let node = &mut *n.state.get();
                node.rc += 1;
                if node.rc == 1 {
                    Self::list_remove(node_ptr);
                }
                node_ptr
            },
            Entry::Vacant(ent) => {
                let node = Box::new(Node {
                    value: OnceCell::new(),
                    state: UnsafeCell::new(NodeState {
                        prev: null(),
                        key: Some(key.clone()),
                        next: null(),
                        rc: 1,
                    }),
                });

                let node_ptr = node.as_ref() as *const Node<K, V>;
                ent.insert(node);
                node_ptr
            }
        };

        unsafe {
            // IMPORTANT: Call user-provided init function *without* lock held.
            drop(guard);
            (*node_ptr).value.get_or_init(|| init(&key));
        }

        CacheHandle {
            lru: self,
            node: node_ptr,
        }
    }
}

#[cfg(test)]
mod tests {
    struct Foo {
        a: u32,
        b: u32,
    }

    #[test]
    fn miri_foo() {
        unsafe {
            let mut foo = Foo { a: 100, b: 200 };
            let p1 = (&mut foo) as *mut Foo;
            let p2 = (&mut foo) as *mut Foo;
            let pp1 = &foo;
            // (*p1).a = 300;
            // (*p2).a = 500;
            // (*p1).a = 600;
            (*p2).a = 700;
            println!("{}", pp1.a);
        }
    }
}
