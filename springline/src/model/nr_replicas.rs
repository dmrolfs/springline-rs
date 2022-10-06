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
pub struct NrReplicas(u32);

impl NrReplicas {
    pub const NONE: Self = Self::new(0);

    pub const fn new(nr: u32) -> Self {
        Self(nr)
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

impl fmt::Display for NrReplicas {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<NrReplicas> for u32 {
    fn from(nr: NrReplicas) -> Self {
        nr.0
    }
}

impl From<NrReplicas> for u64 {
    fn from(nr: NrReplicas) -> Self {
        Self::from(nr.0)
    }
}

impl From<NrReplicas> for TelemetryValue {
    fn from(nr: NrReplicas) -> Self {
        Self::Integer(i64::from(nr.0))
    }
}

impl std::ops::Add for NrReplicas {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0 + rhs.0)
    }
}

impl std::ops::Add<u32> for NrReplicas {
    type Output = Self;

    fn add(self, rhs: u32) -> Self::Output {
        Self(self.0 + rhs)
    }
}

impl std::ops::Sub for NrReplicas {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self(self.0 - rhs.0)
    }
}

impl std::ops::Sub<u32> for NrReplicas {
    type Output = Self;

    fn sub(self, rhs: u32) -> Self::Output {
        Self(self.0 - rhs)
    }
}

impl std::ops::Mul<u32> for NrReplicas {
    type Output = Self;

    fn mul(self, rhs: u32) -> Self::Output {
        Self(self.0 * rhs)
    }
}

impl std::ops::Mul<NrReplicas> for u32 {
    type Output = NrReplicas;

    fn mul(self, rhs: NrReplicas) -> Self::Output {
        NrReplicas(self * rhs.0)
    }
}

impl SaturatingAdd for NrReplicas {
    fn saturating_add(&self, other: &Self) -> Self {
        Self(self.0.saturating_add(other.0))
    }
}

impl SaturatingSub for NrReplicas {
    fn saturating_sub(&self, other: &Self) -> Self {
        Self(self.0.saturating_sub(other.0))
    }
}

#[cfg(test)]
pub fn arb_nr_replicas() -> impl proptest::strategy::Strategy<Value = NrReplicas> {
    use proptest::prelude::*;
    any::<u32>().prop_map(NrReplicas::new)
}
