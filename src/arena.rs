use std::cell::UnsafeCell;

const SHARED_BUFFER_SIZE: usize = 4096;

pub struct Arena {
    internal: UnsafeCell<ArenaInternal>,
}

#[derive(Default)]
struct ArenaInternal {
    shared_pos: usize,
    shared_bufs: Vec<Box<[u8]>>,
    unique_bufs: Vec<Box<[u8]>>,
}

impl Arena {
    pub fn new() -> Self {
        Self {
            internal: UnsafeCell::new(Default::default()),
        }
    }

    pub fn alloc(&self, size: usize) -> &mut [u8] {
        let internal = unsafe { &mut *self.internal.get() };
        if size > SHARED_BUFFER_SIZE {
            internal.unique_bufs.push(vec![0; size].into_boxed_slice());
            internal.unique_bufs.last_mut().unwrap().as_mut()
        } else {
            if internal.shared_bufs.len() == 0 || internal.shared_pos + size > SHARED_BUFFER_SIZE {
                let buf = vec![0; SHARED_BUFFER_SIZE].into_boxed_slice();
                internal.shared_bufs.push(buf);
                internal.shared_pos = 0;
            }

            let buf = internal.shared_bufs.last_mut().unwrap();
            let start = internal.shared_pos;
            let end = start + size;
            let res = &mut buf[start..end];
            internal.shared_pos = end;
            res
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_length() {
        let a = Arena::new();
        for i in 0..100 {
            let len = i + 100;
            assert_eq!(len, a.alloc(len).len());
        }
        for i in 0..10 {
            let len = i + 5000;
            assert_eq!(len, a.alloc(len).len());
        }
    }

    #[test]
    fn test_shared_non_overlapping() {
        let a = Arena::new();
        let sl1 = a.alloc(10);
        let sl2 = a.alloc(10);
        for i in sl1.iter_mut() {
            *i = 10;
        }
        for i in sl2.iter_mut() {
            *i = 20;
        }
        for i in sl1.iter() {
            assert_eq!(*i, 10);
        }
        for i in sl2.iter() {
            assert_eq!(*i, 20);
        }
    }
}
