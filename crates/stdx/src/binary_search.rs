pub trait BinarySearch {
    type Elem;
    fn elem_count(&self) -> usize;
    fn elem(&self, idx: usize) -> Self::Elem;

    fn binary_search(&self, x: &Self::Elem) -> Result<usize, usize>
    where
        Self::Elem: Ord,
    {
        self.binary_search_by(|p| p.cmp(&x))
    }

    fn binary_search_by<F>(&self, mut f: F) -> Result<usize, usize>
    where
        F: FnMut(Self::Elem) -> std::cmp::Ordering,
    {
        use std::cmp::Ordering::*;

        let mut size = self.elem_count();
        if size == 0 {
            return Err(0);
        }
        let mut base = 0usize;
        while size > 1 {
            let half = size / 2;
            let mid = base + half;
            let cmp = f(self.elem(mid));
            base = if cmp == Greater { base } else { mid };
            size -= half;
        }
        let cmp = f(self.elem(base));
        if cmp == Equal {
            Ok(base)
        } else {
            Err(base + (cmp == Less) as usize)
        }
    }
}
