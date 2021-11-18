use std::collections::{HashMap, HashSet};

use oso::{Oso, PolarClass};
use proctor::elements::{PolicySource, PolicySubscription, QueryPolicy, QueryResult, Telemetry};
use proctor::error::PolicyError;
use proctor::phases::collection::TelemetrySubscription;
use proctor::{ProctorContext, SharedString};
use serde::{Deserialize, Serialize};

use super::context::{ClusterStatus, EligibilityContext, TaskStatus};
use crate::phases::{MetricCatalog, UpdateMetrics};
use crate::settings::EligibilitySettings;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct EligibilityTemplateData {
    pub basis: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub cooling_secs: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub stable_secs: Option<u32>,

    #[serde(flatten, skip_serializing_if = "HashMap::is_empty")]
    pub custom: HashMap<String, String>,
}

impl Default for EligibilityTemplateData {
    fn default() -> Self {
        Self {
            basis: format!("{}_basis", EligibilityPolicy::base_template_name()),
            cooling_secs: None,
            stable_secs: None,
            custom: HashMap::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EligibilityPolicy {
    pub required_subscription_fields: HashSet<SharedString>,
    pub optional_subscription_fields: HashSet<SharedString>,
    pub sources: Vec<PolicySource>,
    pub template_data: Option<EligibilityTemplateData>,
}

impl EligibilityPolicy {
    pub fn new(settings: &EligibilitySettings) -> Self {
        let required_subscription_fields = settings
            .required_subscription_fields
            .iter()
            .map(|f| SharedString::from(f.to_string()))
            .collect();
        let optional_subscription_fields = settings
            .optional_subscription_fields
            .iter()
            .map(|f| SharedString::from(f.to_string()))
            .collect();
        Self {
            required_subscription_fields,
            optional_subscription_fields,
            sources: settings.policies.clone(),
            template_data: settings.template_data.clone(),
        }
    }
}

impl PolicySubscription for EligibilityPolicy {
    type Requirements = EligibilityContext;

    fn do_extend_subscription(&self, subscription: TelemetrySubscription) -> TelemetrySubscription {
        let update_fn = Self::Requirements::update_metrics_for(subscription.name());
        subscription.with_update_metrics_fn(update_fn)
    }
}

impl QueryPolicy for EligibilityPolicy {
    type Args = (Self::Item, Self::Context);
    type Context = EligibilityContext;
    type Item = MetricCatalog;
    type TemplateData = EligibilityTemplateData;

    fn initialize_policy_engine(&mut self, oso: &mut Oso) -> Result<(), PolicyError> {
        Telemetry::initialize_policy_engine(oso)?;

        oso.register_class(
            EligibilityContext::get_polar_class_builder()
                .add_method("custom", ProctorContext::custom)
                .build(),
        )?;
        oso.register_class(
            TaskStatus::get_polar_class_builder()
                .name("TaskStatus")
                .add_method("last_failure_within_seconds", TaskStatus::last_failure_within_seconds)
                .build(),
        )?;
        oso.register_class(
            ClusterStatus::get_polar_class_builder()
                .name("ClusterStatus")
                .add_method(
                    "last_deployment_within_seconds",
                    ClusterStatus::last_deployment_within_seconds,
                )
                .build(),
        )?;

        Ok(())
    }

    fn make_query_args(&self, item: &Self::Item, context: &Self::Context) -> Self::Args {
        (item.clone(), context.clone())
    }

    fn query_policy(&self, engine: &Oso, args: Self::Args) -> Result<QueryResult, PolicyError> {
        QueryResult::from_query(engine.query_rule("eligible", args)?)
    }

    fn base_template_name() -> &'static str {
        "eligibility"
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

    #[test]
    fn test_ser_eligibility_setting() {
        let settings = EligibilitySettings {
            policies: vec![
                assert_ok!(PolicySource::from_template_file("./resources/eligibility.polar")),
                assert_ok!(PolicySource::from_template_file("./resources/eligibility_basis.polar")),
            ],
            template_data: Some(EligibilityTemplateData {
                basis: "eligibility_basis".to_string(),
                cooling_secs: Some(900),
                stable_secs: Some(900),
                custom: maplit::hashmap! { "foo".to_string() => "bar".to_string(), },
            }),
            ..EligibilitySettings::default()
        };

        let rep = assert_ok!(ron::ser::to_string_pretty(&settings, ron::ser::PrettyConfig::default()));
        let mut ron_deser = assert_ok!(ron::Deserializer::from_str(&rep));
        let mut json_rep = vec![];
        let mut json_ser = serde_json::Serializer::pretty(json_rep);
        assert_ok!(serde_transcode::transcode(&mut ron_deser, &mut json_ser));
        let json_rep = assert_ok!(String::from_utf8(json_ser.into_inner()));
        let expected_json = r##"|{
            |  "policies": [
            |    {
            |      "source": "file",
            |      "policy": {
            |        "path": "./resources/eligibility.polar",
            |        "is_template": true
            |      }
            |    },
            |    {
            |      "source": "file",
            |      "policy": {
            |        "path": "./resources/eligibility_basis.polar",
            |        "is_template": true
            |      }
            |    }
            |  ],
            |  "template_data": {
            |    "basis": "eligibility_basis",
            |    "cooling_secs": 900,
            |    "stable_secs": 900,
            |    "foo": "bar"
            |  }
            |}"##
            .trim_margin_with("|")
            .unwrap();

        assert_eq!(json_rep, expected_json);

        let actual: EligibilitySettings = assert_ok!(serde_json::from_str(&json_rep));
        assert_eq!(actual, settings);
    }

    #[test]
    fn test_deser_eligibility_setting() {
        let rep = r##"|{
            |  "policies": [
            |    {
            |      "source": "file",
            |      "policy": {
            |        "path": "./resources/eligibility.polar",
            |        "is_template": true
            |      }
            |    },
            |    {
            |      "source": "file",
            |      "policy": {
            |        "path": "./resources/eligibility_basis.polar",
            |        "is_template": true
            |      }
            |    }
            |  ],
            |  "template_data": {
            |    "basis": "eligibility_basis",
            |    "cooling_secs": 900,
            |    "stable_secs": 900,
            |    "foo": "bar"
            |  }
            |}"##
            .trim_margin_with("|")
            .unwrap();

        let actual: EligibilitySettings = assert_ok!(serde_json::from_str(&rep));
        assert_eq!(
            actual,
            EligibilitySettings {
                policies: vec![
                    assert_ok!(PolicySource::from_template_file("./resources/eligibility.polar")),
                    assert_ok!(PolicySource::from_template_file("./resources/eligibility_basis.polar")),
                ],
                template_data: Some(EligibilityTemplateData {
                    basis: "eligibility_basis".to_string(),
                    cooling_secs: Some(900),
                    stable_secs: Some(900),
                    custom: maplit::hashmap! { "foo".to_string() => "bar".to_string(), },
                }),
                ..EligibilitySettings::default()
            }
        )
    }
}
