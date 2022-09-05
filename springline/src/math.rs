use std::num::TryFromIntError;

pub fn try_f64_to_u32(value: f64) -> u32 {
    value.round().rem_euclid(2_f64.powi(32)) as u32
}

pub fn try_u64_to_f64(value: u64) -> Result<f64, TryFromIntError> {
    u32::try_from(value).map(f64::from)
}
