use std::num::TryFromIntError;

#[inline]
pub fn try_f64_to_u32(value: f64) -> u32 {
    value.round().rem_euclid(2_f64.powi(32)) as u32
}

#[inline]
pub fn try_u64_to_f64(value: u64) -> Result<f64, TryFromIntError> {
    u32::try_from(value).map(f64::from)
}

#[inline]
pub fn saturating_usize_to_u32(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

#[inline]
pub fn saturating_u32_to_usize(value: u32) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

#[inline]
pub fn saturating_u64_to_u32(value: u64) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}
