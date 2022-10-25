use frunk::{Monoid, Semigroup};
use std::cmp::Ordering;

#[derive(Debug)]
pub struct Saturating<T>(pub T);

impl<T: Clone> Clone for Saturating<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: Copy + Clone> Copy for Saturating<T> {}

impl<T: PartialEq> PartialEq for Saturating<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<T: Eq + PartialEq> Eq for Saturating<T> {}

impl<T: PartialOrd> PartialOrd for Saturating<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl<T: Ord + PartialOrd> Ord for Saturating<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

macro_rules! int_monoid_saturating_imps {
    ($($tr:ty), *) => {
        $(
            impl Semigroup for Saturating<$tr> {
                fn combine(&self, other: &Self) -> Self {
                    let sum = self.0.saturating_add(other.0);
                    Self(sum)
                }
            }

            impl Semigroup for Saturating<Option<$tr>> {
                fn combine(&self, other: &Self) -> Self {
                    let result = match (self.0.map(Saturating), other.0.map(Saturating)) {
                        (Some(x), Some(y)) => Some(x.combine(&y)),
                        (Some(x), None) => Some(x),
                        (None, Some(y)) => Some(y),
                        (None, None) => None,
                    };
                    Self(result.map(|r| r.0))
                }
            }

            impl Monoid for Saturating<$tr> {
                fn empty() -> Self {
                    Self(<$tr as Monoid>::empty())
                }
            }

            impl Monoid for Saturating<Option<$tr>> {
                fn empty() -> Self {
                    Self(None)
                }
            }
        )*
    }
}

int_monoid_saturating_imps!(i8, i16, i32, i64, u8, u16, u32, u64, isize, usize);

impl Semigroup for Saturating<f32> {
    fn combine(&self, other: &Self) -> Self {
        let sum = self.0 + other.0;
        Self(sum)
    }
}

impl Semigroup for Saturating<Option<f32>> {
    fn combine(&self, other: &Self) -> Self {
        let result = match (self.0.map(Saturating), other.0.map(Saturating)) {
            (Some(x), Some(y)) => Some(x.combine(&y)),
            (Some(x), None) => Some(x),
            (None, Some(y)) => Some(y),
            (None, None) => None,
        };
        Self(result.map(|r| r.0))
    }
}

impl Monoid for Saturating<f32> {
    fn empty() -> Self {
        Self(<f32 as Monoid>::empty())
    }
}

impl Monoid for Saturating<Option<f32>> {
    fn empty() -> Self {
        Self(None)
    }
}

impl Semigroup for Saturating<f64> {
    fn combine(&self, other: &Self) -> Self {
        let sum = self.0 + other.0;
        Self(sum)
    }
}

impl Semigroup for Saturating<Option<f64>> {
    fn combine(&self, other: &Self) -> Self {
        let result = match (self.0.map(Saturating), other.0.map(Saturating)) {
            (Some(x), Some(y)) => Some(x.combine(&y)),
            (Some(x), None) => Some(x),
            (None, Some(y)) => Some(y),
            (None, None) => None,
        };
        Self(result.map(|r| r.0))
    }
}

impl Monoid for Saturating<f64> {
    fn empty() -> Self {
        Self(<f64 as Monoid>::empty())
    }
}

impl Monoid for Saturating<Option<f64>> {
    fn empty() -> Self {
        Self(None)
    }
}
