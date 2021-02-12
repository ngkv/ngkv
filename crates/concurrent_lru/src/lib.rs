use std::{
    collections::{hash_map::Entry, HashMap},
    hash::Hash,
    marker::PhantomData,
    ops::{Deref, DerefMut},
    ptr::{null_mut, NonNull},
    sync::{
        atomic::{AtomicU32, Ordering},
        Mutex, MutexGuard,
    },
    todo,
};

use once_cell::sync::OnceCell;

struct Node<K, V> {
    rc: u32,
    key: Option<K>,
    value: OnceCell<V>,
    in_lru_list: bool,
    prev: *mut Node<K, V>,
    next: *mut Node<K, V>,
}

struct LruState<K, V> {
    capacity: usize,
    map: HashMap<K, Box<Node<K, V>>>,

    /// Dummy head node of the circular linked list.
    list_size: usize,
    list_dummy: Box<Node<K, V>>,
}

pub struct Lru<K, V> {
    state: Mutex<LruState<K, V>>,
}

pub struct CacheHandle<'a, K: Hash + Eq + Clone, V> {
    lru: &'a Lru<K, V>,
    node: *mut Node<K, V>,
}

impl<'a, K: Hash + Eq + Clone, V> CacheHandle<'a, K, V> {
    pub fn value(&self) -> &V {

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
    unsafe fn link(cur: *mut Node<K, V>, next: *mut Node<K, V>) {
        (*cur).next = next;
        (*next).prev = cur;
    }

    unsafe fn list_insert(dummy: *mut Node<K, V>, node: *mut Node<K, V>) {
        let prev = (*dummy).prev;
        let next = (*dummy).next;
        Self::link(prev, node);
        Self::link(node, next);
    }

    unsafe fn list_remove(node: *mut Node<K, V>) {
        let node = &mut *node;
        let prev = node.prev;
        let next = node.next;
        Self::link(prev, next);
    }

    pub fn new(capacity: usize) -> Self {
        let mut dummy = Box::new(Node {
            prev: null_mut(),
            next: null_mut(),
            key: None,
            value: OnceCell::new(),
            in_lru_list: true,
            rc: 0,
        });

        dummy.next = dummy.as_mut();
        dummy.prev = dummy.as_mut();

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

        let state = guard.deref_mut();
        let target_size = if evict_all { 0 } else { state.capacity };

        unsafe {
            while state.list_size > target_size {
                let oldest_ptr = state.list_dummy.prev;
                assert!(oldest_ptr != state.list_dummy.as_mut());

                // Remove node from LRU list.
                let oldest = &mut *oldest_ptr;
                assert!(oldest.in_lru_list && oldest.rc == 0);
                state.list_size -= 1;
                oldest.in_lru_list = false;
                Self::list_remove(oldest);

                // Remove node from hash map.
                let node = state.map.remove(oldest.key.as_ref().unwrap()).unwrap();
                evict_nodes.push(node);
            }
        }

        // IMPORTANT: Drop user value without lock held.
        drop(guard);
        drop(evict_nodes);
    }

    fn unref(mut guard: MutexGuard<LruState<K, V>>, node: *mut Node<K, V>) {
        let state = guard.deref_mut();
        let node = unsafe { &mut *node };
        node.rc.checked_sub(1).unwrap();
        if node.rc == 0 {
            assert!(!node.in_lru_list);
            node.in_lru_list = true;
            unsafe {
                Self::list_insert(state.list_dummy.as_mut(), node);
            }
            Self::maybe_evict(guard, false);
        }
    }

    pub fn get_or_init<'a>(&'a self, key: K, init: impl FnOnce(&K) -> V) -> CacheHandle<'a, K, V> {
        let mut guard = self.state.lock().unwrap();
        let state = guard.deref_mut();
        let node_ptr = match state.map.entry(key.clone()) {
            Entry::Occupied(mut ent) => unsafe {
                let node = ent.get_mut().deref_mut();
                node.rc += 1;
                if node.in_lru_list {
                    Self::list_remove(node);
                }
                node as *mut Node<K, V>
            },
            Entry::Vacant(ent) => {
                let mut node = Box::new(Node {
                    prev: null_mut(),
                    key: Some(key.clone()),
                    next: null_mut(),
                    value: OnceCell::new(),
                    in_lru_list: false,
                    rc: 1,
                });

                let ptr = node.as_mut() as *mut Node<K, V>;
                ent.insert(node);
                ptr
            }
        };

        unsafe {
            // Dereference node_ptr with lock held to avoid breaking the alias
            // rule.
            let value = &(*node_ptr).value;
            drop(guard);

            // IMPORTANT: Call user-provided init function *without* lock held.
            value.get_or_init(|| init(&key));
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
