use std::ops::Bound;

pub struct BoundInfo<'a, T> {
    pub content: &'a T,
    pub inclusive: bool,
}

pub trait BoundExt<T> {
    /// Get content & whether the bound is inclusive.
    fn info(&self) -> Option<BoundInfo<T>>;
    fn as_ref(&self) -> Bound<&T>;
    fn is_left_bound_of(&self, x: &T) -> bool where T: PartialOrd;
    fn is_right_bound_of(&self, x: &T) -> bool where T: PartialOrd;
}

impl<T> BoundExt<T> for Bound<T> {
    fn info(&self) -> Option<BoundInfo<T>> {
        match &self {
            Bound::Included(t) => Some(BoundInfo {
                content: t,
                inclusive: true,
            }),
            Bound::Excluded(t) => Some(BoundInfo {
                content: t,
                inclusive: false,
            }),
            Bound::Unbounded => None,
        }
    }

    fn as_ref(&self) -> Bound<&T> {
        match &self {
            Bound::Included(t) => Bound::Included(t),
            Bound::Excluded(t) => Bound::Excluded(t),
            Bound::Unbounded => Bound::Unbounded,
        }
    }

    fn is_left_bound_of(&self, x: &T) -> bool
    where T: PartialOrd
    {
        match &self {
            Bound::Included(t) => t < x,
            Bound::Excluded(t) => t <= x,
            Bound::Unbounded => true,
        }
    }

    fn is_right_bound_of(&self, x: &T) -> bool
    where T: PartialOrd
    {
        match &self {
            Bound::Included(t) => t >= x,
            Bound::Excluded(t) => t > x,
            Bound::Unbounded => true,
        }
    }
}
