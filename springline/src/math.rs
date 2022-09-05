pub fn try_f64_to_u32(value: f64) -> Result<u32, anyhow::Error> {
    Ok(value.round().rem_euclid(2_f64.powi(32)) as u32)
}

pub fn try_u64_to_f64(value: u64) -> Result<f64, anyhow::Error> {
    u32::try_from(value).map(f64::from).map_err(|err| err.into())
}