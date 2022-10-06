use num_traits::{SaturatingAdd, SaturatingSub};
use oso::PolarClass;
use proctor::elements::TelemetryValue;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(
    PolarClass, Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(transparent)]
#[repr(transparent)]
pub struct Parallelism(u32);

impl Parallelism {
    pub const MIN: Parallelism = Parallelism::new(1);

    pub const fn new(p: u32) -> Self {
        Self(p)
    }

    pub fn min(lhs: Self, rhs: Self) -> Self {
        Self(u32::min(lhs.into(), rhs.into()))
    }

    pub fn max(rhs: Self, lhs: Self) -> Self {
        Self(u32::max(lhs.into(), rhs.into()))
    }

    pub const fn as_u32(&self) -> u32 {
        self.0
    }

    pub fn as_f64(&self) -> f64 {
        f64::from(self.0)
    }
}

impl fmt::Display for Parallelism {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Parallelism> for u32 {
    fn from(p: Parallelism) -> Self {
        p.0
    }
}

impl From<Parallelism> for u64 {
    fn from(p: Parallelism) -> Self {
        Self::from(p.0)
    }
}

impl From<Parallelism> for TelemetryValue {
    fn from(p: Parallelism) -> Self {
        Self::Integer(i64::from(p.0))
    }
}

impl std::ops::Add for Parallelism {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0 + rhs.0)
    }
}

impl std::ops::Add<u32> for Parallelism {
    type Output = Self;

    fn add(self, rhs: u32) -> Self::Output {
        Self(self.0 + rhs)
    }
}

impl std::ops::Sub for Parallelism {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self(self.0 - rhs.0)
    }
}

impl std::ops::Sub<u32> for Parallelism {
    type Output = Self;

    fn sub(self, rhs: u32) -> Self::Output {
        Self(self.0 - rhs)
    }
}

impl std::ops::Mul<u32> for Parallelism {
    type Output = Self;

    fn mul(self, rhs: u32) -> Self::Output {
        Self(self.0 * rhs)
    }
}

impl std::ops::Mul<Parallelism> for u32 {
    type Output = Parallelism;

    fn mul(self, rhs: Parallelism) -> Self::Output {
        Parallelism(self * rhs.0)
    }
}

impl SaturatingAdd for Parallelism {
    fn saturating_add(&self, other: &Self) -> Self {
        Self(self.0.saturating_add(other.0))
    }
}

impl SaturatingSub for Parallelism {
    fn saturating_sub(&self, other: &Self) -> Self {
        Self(self.0.saturating_sub(other.0))
    }
}

#[cfg(test)]
pub fn arb_parallelism() -> impl proptest::strategy::Strategy<Value = Parallelism> {
    use proptest::prelude::*;
    any::<u32>().prop_map(Parallelism::new)
}
