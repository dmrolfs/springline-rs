use std::collections::HashMap;

use proctor::elements::telemetry;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GovernanceRuleSettings {
    pub min_cluster_size: u16,
    pub max_cluster_size: u16,
    pub min_scaling_step: u16,
    pub max_scaling_step: u16,
    pub custom: telemetry::TableType,
}
