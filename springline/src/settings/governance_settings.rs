use std::collections::HashMap;

use proctor::elements::{telemetry, PolicySettings};
use serde::{Deserialize, Serialize};

use crate::phases::governance::GovernanceTemplateData;

pub type GovernancePolicySettings = PolicySettings<GovernanceTemplateData>;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(PartialEq))]
pub struct GovernanceSettings {
    pub policy: GovernancePolicySettings,
    pub rules: GovernanceRuleSettings,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GovernanceRuleSettings {
    pub min_cluster_size: u16,
    pub max_cluster_size: u16,
    pub min_scaling_step: u16,
    pub max_scaling_step: u16,
    pub custom: telemetry::TableType,
}

impl Default for GovernanceRuleSettings {
    fn default() -> Self {
        Self {
            min_cluster_size: 0,
            max_cluster_size: 10,
            min_scaling_step: 2,
            max_scaling_step: 5,
            custom: HashMap::default(),
        }
    }
}
