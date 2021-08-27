use std::time::Duration;
use crate::phases::plan::PerformanceRepositorySettings;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanSettings {
    pub min_scaling_step: u16,
    pub restart: Duration,
    pub max_catch_up: Duration,
    pub recovery_valid: Duration,
    pub performance_repository: PerformanceRepositorySettings,
}