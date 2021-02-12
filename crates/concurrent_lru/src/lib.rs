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

struct Node<V> {
    rc: u32,
    value: OnceCell<V>,
    in_lru_list: bool,
    prev: *mut Node<V>,
    next: *mut Node<V>,
}

struct LruState<K, V> {
    capacity: usize,
    map: HashMap<K, Box<Node<V>>>,

    /// Dummy head node of the circular linked list.
    dummy: Box<Node<V>>,
}

pub struct Lru<K, V> {
    state: Mutex<LruState<K, V>>,
}

pub struct CacheHandle<'a, K, V> {
    lru: &'a Lru<K, V>,
    node: *mut Node<V>,
}

impl<K: Hash + Eq + Clone, V> Lru<K, V> {
    unsafe fn link(cur: *mut Node<V>, next: *mut Node<V>) {
        (*cur).next = next;
        (*next).prev = cur;
    }

    unsafe fn insert_head(dummy: *mut Node<V>, node: *mut Node<V>) {
        let prev = (*dummy).prev;
        let next = (*dummy).next;
        Self::link(prev, node);
        Self::link(node, next);
    }

    unsafe fn list_remove(node: *mut Node<V>) {
        let node = &mut *node;
        assert!(node.in_lru_list);
        node.in_lru_list = false;
        let prev = node.prev;
        let next = node.next;
        Self::link(prev, next);
    }

    unsafe fn bring_to_head(dummy: *mut Node<V>, cur: *mut Node<V>) {
        let prev = (*cur).prev;
        let next = (*cur).next;
        Self::link(prev, next);
        Self::insert_head(dummy, cur);
    }

    pub fn new(capacity: usize) -> Self {
        let mut dummy = Box::new(Node {
            prev: null_mut(),
            next: null_mut(),
            value: OnceCell::new(),
            in_cache: true,
            rc: 0,
        });

        dummy.next = dummy.as_mut();
        dummy.prev = dummy.as_mut();

        Self {
            state: Mutex::new(LruState {
                map: Default::default(),
                capacity,
                dummy,
            }),
        }
    }

    fn maybe_evict_unused(mut guard: MutexGuard<LruState<K, V>>) {
        let state = guard.deref_mut();
        if state.map.len() > state.capacity {
            state.dummy
        }
    }

    pub fn get_or_init<'a>(&'a self, key: K, init: impl FnOnce(&K) -> V) -> CacheHandle<'a, K, V> {
        let mut guard = self.state.lock().unwrap();
        let state = guard.deref_mut();
        let node_ptr = match state.map.entry(key.clone()) {
            Entry::Occupied(mut ent) => unsafe {
                let node_ptr = ent.get_mut().deref_mut() as *mut Node<V>;
                Self::bring_to_head(state.dummy.deref_mut(), node_ptr);
                node_ptr
            },
            Entry::Vacant(ent) => {
                let mut node = Box::new(Node {
                    prev: null_mut(),
                    next: null_mut(),
                    value: OnceCell::new(),
                    rc: AtomicU32::new(0),
                });

                let node_ptr = node.deref_mut() as *mut Node<V>;
                unsafe {
                    Self::insert_head(state.dummy.deref_mut(), node_ptr);
                }
                ent.insert(node);
                node_ptr
            }
        };

        unsafe {
            // Dereference node_ptr with lock held to avoid breaking the alias
            // rule.
            let value = &(*node_ptr).value;
            (*node_ptr).rc.fetch_add(1, Ordering::Relaxed);
            Self::maybe_evict_unused(guard);

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
