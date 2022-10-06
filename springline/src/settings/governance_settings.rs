use std::collections::HashMap;

use proctor::elements::{telemetry, PolicySettings};
use serde::{Deserialize, Serialize};

pub type GovernancePolicySettings = PolicySettings<GovernanceTemplateData>;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(PartialEq))]
pub struct GovernanceSettings {
    pub policy: GovernancePolicySettings,
    pub rules: GovernanceRuleSettings,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GovernanceRuleSettings {
    pub min_parallelism: u32,
    pub max_parallelism: u32,
    pub min_cluster_size: u32,
    pub max_cluster_size: u32,
    pub min_scaling_step: u32,
    pub max_scaling_step: u32,
    pub custom: telemetry::TableType,
}

impl Default for GovernanceRuleSettings {
    fn default() -> Self {
        Self {
            min_parallelism: 1,
            max_parallelism: 10,
            min_cluster_size: 0,
            max_cluster_size: 10,
            min_scaling_step: 2,
            max_scaling_step: 5,
            custom: HashMap::default(),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct GovernanceTemplateData {
    #[serde(flatten, skip_serializing_if = "HashMap::is_empty")]
    pub custom: HashMap<String, String>,
}

#[cfg(test)]
mod tests {
    use claim::*;
    use pretty_assertions::assert_eq;
    use proctor::elements::PolicySource;
    use trim_margin::MarginTrimmable;

    use super::*;

    #[test]
    fn test_ser_governance_setting() {
        let settings = GovernanceSettings {
            policy: GovernancePolicySettings {
                policies: vec![assert_ok!(PolicySource::from_template_file(
                    "./resources/governance.polar"
                ))],
                template_data: None,
                ..GovernancePolicySettings::default()
            },
            ..GovernanceSettings::default()
        };

        let actual_rep = assert_ok!(ron::ser::to_string_pretty(
            &settings,
            ron::ser::PrettyConfig::default()
        ));

        assert_eq!(
            actual_rep,
            r##"
            | (
            |     policy: (
            |         policies: [
            |             (
            |                 source: "file",
            |                 policy: (
            |                     path: "./resources/governance.polar",
            |                     is_template: true,
            |                 ),
            |             ),
            |         ],
            |     ),
            |     rules: (
            |         min_parallelism: 1,
            |         max_parallelism: 10,
            |         min_cluster_size: 0,
            |         max_cluster_size: 10,
            |         min_scaling_step: 2,
            |         max_scaling_step: 5,
            |         custom: {},
            |     ),
            | )"##
                .trim_margin_with("| ")
                .unwrap()
        );

        let actual: GovernanceSettings = assert_ok!(ron::from_str(&actual_rep));
        assert_eq!(actual, settings);
    }
}
