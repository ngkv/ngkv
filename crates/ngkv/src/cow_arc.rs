use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
    todo,
};

pub struct CowArc<T: Clone> {
    arc: Arc<T>,
}

impl<T: Clone> CowArc<T> {
    pub fn new(data: T) -> Self {
        Self {
            arc: Arc::new(data),
        }
    }
}

impl<T: Clone> Clone for CowArc<T> {
    fn clone(&self) -> Self {
        Self {
            arc: self.arc.clone(),
        }
    }
}

impl<T: Clone> Deref for CowArc<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &*self.arc
    }
}

impl<T: Clone> DerefMut for CowArc<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        loop {
            if let Some(t) = Arc::get_mut(&mut self.arc) {
                break unsafe { &mut *(t as *mut T) };
            } else {
                let data = self.arc.as_ref().clone();
                self.arc = Arc::new(data);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn cow_arc_basic() {}
}
