use crate::phases::plan::{PerformanceRepositorySettings, SpikeSettings};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlanSettings {
    pub min_scaling_step: u16,
    pub restart: Duration,
    pub max_catch_up: Duration,
    pub recovery_valid: Duration,
    pub performance_repository: PerformanceRepositorySettings,
    pub window: usize,
    pub spike: SpikeSettings,
}
