use std::{
    collections::{hash_map::Entry, HashMap},
    hash::Hash,
    ops::DerefMut,
    ptr::NonNull,
    sync::{atomic::AtomicU32, Mutex},
    todo,
};

struct Node<V> {
    rc: AtomicU32,
    prev: NonNull<Node<V>>,
    next: NonNull<Node<V>>,
    value: V,
}

struct LruState<K, V> {
    map: HashMap<K, Box<Node<V>>>,

    /// Dummy head node of the circular linked list.
    dummy: Box<Node<V>>,
}

pub struct Lru<K, V> {
    state: Mutex<LruState<K, V>>,
}

pub struct CacheHandle<'a, K, V> {
    lru: &'a Lru<K, V>,
}

impl<K: Hash + Eq, V> Lru<K, V> {
    unsafe fn list_insert_head(dummy: NonNull<Node<V>>, node: NonNull<Node<V>>) {

    }

    pub fn get_or_init<'a>(&'a self, key: K, init: impl FnOnce(&K) -> V) -> CacheHandle<'a, K, V> {
        let mut guard = self.state.lock().unwrap();
        let state = guard.deref_mut();
        let node_ptr = match state.map.entry(key) {
            Entry::Occupied(mut ent) => {
                let cur_node = ent.get_mut();
                let cur_ptr = unsafe { NonNull::new_unchecked(cur_node.as_mut()) };
                let 
                cur_node.prev

                todo!()
            }
            Entry::Vacant(ent) => {
                let value = init(ent.key());
                // Insert node after the dummy head node.
                let mut prev_ptr = unsafe { NonNull::new_unchecked(state.dummy.as_mut()) };
                let mut next_ptr = state.dummy.next;
                let mut cur_node = Box::new(Node {
                    prev: prev_ptr,
                    next: next_ptr,
                    value,
                    rc: AtomicU32::new(0),
                });
                let cur_ptr = unsafe { NonNull::new_unchecked(cur_node.as_mut()) };
                unsafe {
                    prev_ptr.as_mut().next = cur_ptr;
                    next_ptr.as_mut().prev = cur_ptr;
                }
                ent.insert(cur_node);
            }
        };
        todo!()
    }
}
