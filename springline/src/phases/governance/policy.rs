use std::collections::{HashMap, HashSet};

use oso::{Oso, PolarClass, PolarValue};
use proctor::elements::{
    PolicyContributor, PolicySource, PolicySubscription, QueryPolicy, QueryResult, Telemetry,
};
use proctor::error::PolicyError;
use proctor::phases::sense::TelemetrySubscription;
use proctor::{ProctorContext, SharedString};
use serde::{Deserialize, Serialize};

use super::context::GovernanceContext;
use crate::flink::MetricCatalog;
use crate::metrics::UpdateMetrics;
use crate::phases::plan::ScalePlan;
use crate::settings::GovernanceSettings;

pub const ADJUSTED_TARGET_PARALLELISM: &str = "adjusted_target_parallelism";
pub const ADJUSTED_TARGET_NR_TASK_MANAGERS: &str = "adjusted_target_nr_task_managers";

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct GovernanceTemplateData {
    #[serde(flatten, skip_serializing_if = "HashMap::is_empty")]
    pub custom: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GovernancePolicy {
    pub required_subscription_fields: HashSet<SharedString>,
    pub optional_subscription_fields: HashSet<SharedString>,
    pub sources: Vec<PolicySource>,
    pub template_data: Option<GovernanceTemplateData>,
}

impl GovernancePolicy {
    pub fn new(settings: &GovernanceSettings) -> Self {
        let required_subscription_fields = settings
            .policy
            .required_subscription_fields
            .iter()
            .map(|f| SharedString::from(f.to_string()))
            .collect();
        let optional_subscription_fields = settings
            .policy
            .optional_subscription_fields
            .iter()
            .map(|f| SharedString::from(f.to_string()))
            .collect();
        Self {
            required_subscription_fields,
            optional_subscription_fields,
            sources: settings.policy.policies.clone(),
            template_data: settings.policy.template_data.clone(),
        }
    }
}

impl PolicySubscription for GovernancePolicy {
    type Requirements = GovernanceContext;

    fn do_extend_subscription(&self, subscription: TelemetrySubscription) -> TelemetrySubscription {
        let update_fn = Self::Requirements::update_metrics_for(subscription.name());
        subscription.with_update_metrics_fn(update_fn)
    }
}

impl QueryPolicy for GovernancePolicy {
    type Args = (Self::Item, Self::Context, PolarValue, PolarValue);
    type Context = GovernanceContext;
    type Item = ScalePlan;
    type TemplateData = GovernanceTemplateData;

    fn initialize_policy_engine(&self, engine: &mut Oso) -> Result<(), PolicyError> {
        Telemetry::register_with_policy_engine(engine)?;
        MetricCatalog::register_with_policy_engine(engine)?;

        engine.register_class(
            GovernanceContext::get_polar_class_builder()
                .add_method("custom", ProctorContext::custom)
                .build(),
        )?;

        Ok(())
    }

    fn make_query_args(&self, item: &Self::Item, context: &Self::Context) -> Self::Args {
        (
            item.clone(),
            context.clone(),
            PolarValue::Variable(ADJUSTED_TARGET_PARALLELISM.to_string()),
            PolarValue::Variable(ADJUSTED_TARGET_NR_TASK_MANAGERS.to_string()),
        )
    }

    fn query_policy(&self, engine: &Oso, args: Self::Args) -> Result<QueryResult, PolicyError> {
        QueryResult::from_query(engine.query_rule("accept", args)?)
    }

    fn base_template_name() -> &'static str {
        "governance"
    }

    fn policy_template_data(&self) -> Option<&Self::TemplateData> {
        self.template_data.as_ref()
    }

    fn policy_template_data_mut(&mut self) -> Option<&mut Self::TemplateData> {
        self.template_data.as_mut()
    }

    fn sources(&self) -> &[PolicySource] {
        self.sources.as_slice()
    }

    fn sources_mut(&mut self) -> &mut Vec<PolicySource> {
        &mut self.sources
    }
}

#[cfg(test)]
mod tests {
    use claim::*;
    use pretty_assertions::assert_eq;
    use trim_margin::MarginTrimmable;

    use super::*;
    use crate::settings::GovernancePolicySettings;

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
