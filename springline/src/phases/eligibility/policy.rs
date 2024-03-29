use std::collections::{HashMap, HashSet};

use crate::Env;
use once_cell::sync::Lazy;
use oso::{Oso, PolarClass, PolarValue};
use proctor::elements::{
    PolicyContributor, PolicySource, PolicySubscription, QueryPolicy, QueryResult, Telemetry,
};
use proctor::error::PolicyError;
use proctor::phases::sense::TelemetrySubscription;
use proctor::ProctorContext;
use prometheus::{IntCounterVec, Opts};
use serde::{Deserialize, Serialize};

use super::context::{ClusterStatus, EligibilityContext, JobStatus};
use crate::flink::AppDataWindow;
use crate::metrics::UpdateMetrics;
use crate::phases::eligibility::{EligibilityData, EligibilityDataT};
use crate::phases::REASON;
use crate::settings::EligibilitySettings;

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct EligibilityTemplateData {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_extension: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub cooling_secs: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub stable_secs: Option<u32>,

    #[serde(flatten, skip_serializing_if = "HashMap::is_empty")]
    pub custom: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EligibilityPolicy {
    pub required_subscription_fields: HashSet<String>,
    pub optional_subscription_fields: HashSet<String>,
    pub sources: Vec<PolicySource>,
    pub template_data: Option<EligibilityTemplateData>,
}

impl EligibilityPolicy {
    pub fn new(settings: &EligibilitySettings) -> Self {
        Self {
            required_subscription_fields: settings.required_subscription_fields.clone(),
            optional_subscription_fields: settings.optional_subscription_fields.clone(),
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

const INELIGIBLE: &str = "ineligible";

impl QueryPolicy for EligibilityPolicy {
    type Item = EligibilityData;
    type Context = Env<EligibilityContext>;
    type Args = (EligibilityDataT, EligibilityContext, PolarValue);
    type TemplateData = EligibilityTemplateData;

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

    fn initialize_policy_engine(&self, oso: &mut Oso) -> Result<(), PolicyError> {
        Telemetry::register_with_policy_engine(oso)?;
        AppDataWindow::register_with_policy_engine(oso)?;

        oso.register_class(
            EligibilityContext::get_polar_class_builder()
                .add_method("custom", ProctorContext::custom)
                .build(),
        )?;
        oso.register_class(
            JobStatus::get_polar_class_builder()
                .name("TaskStatus")
                .add_method(
                    "last_failure_within_seconds",
                    JobStatus::last_failure_within_seconds,
                )
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
        (
            item.clone().into_inner(),
            context.clone().into_inner(),
            PolarValue::Variable(REASON.to_string()),
        )
    }

    fn query_policy(&self, engine: &Oso, args: Self::Args) -> Result<QueryResult, PolicyError> {
        if args.1.cluster.is_rescaling {
            tracing::info!(item=?args.0, context=?args.1, "Cluster is rescaling, skipping eligibility policy check");
            Ok(QueryResult {
                passed: false,
                bindings: maplit::hashmap! { REASON.to_string() => vec!["rescaling".into()] },
            })
        } else {
            QueryResult::from_query(engine.query_rule(INELIGIBLE, args)?).map(|mut query_result| {
                // goal is to pass on ELIGIBLE; however in order to surface reasons on the "negative"
                // it's cheaper to query on negative then reverse the passed status
                query_result.passed = !query_result.passed;
                if !query_result.passed {
                    if let Some(reason) =
                        query_result.bindings.get("reason").and_then(|rs| rs.first())
                    {
                        ELIGIBILITY_POLICY_INELIGIBLE_DECISIONS_COUNT
                            .with_label_values(&[&reason.to_string()])
                            .inc();
                    }
                }

                query_result
            })
        }
    }
}

pub static ELIGIBILITY_POLICY_INELIGIBLE_DECISIONS_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "eligibility_policy_ineligible_decisions_count",
            "number of ineligible decisions",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
        &["reason"],
    )
    .expect("failed creating eligibility_policy_ineligible_decisions_count metric")
});

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
                assert_ok!(PolicySource::from_template_file(
                    "./resources/eligibility.polar"
                )),
                assert_ok!(PolicySource::from_template_file(
                    "./resources/eligibility_ext.polar"
                )),
            ],
            template_data: Some(EligibilityTemplateData {
                policy_extension: Some("eligibility_ext".to_string()),
                cooling_secs: Some(900),
                stable_secs: Some(900),
                custom: maplit::hashmap! { "foo".to_string() => "bar".to_string(), },
            }),
            ..EligibilitySettings::default()
        };

        let rep = assert_ok!(ron::ser::to_string_pretty(
            &settings,
            ron::ser::PrettyConfig::default()
        ));
        let mut ron_deser = assert_ok!(ron::Deserializer::from_str(&rep));
        let json_rep = vec![];
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
            |        "path": "./resources/eligibility_ext.polar",
            |        "is_template": true
            |      }
            |    }
            |  ],
            |  "template_data": {
            |    "policy_extension": "eligibility_ext",
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
            |        "path": "./resources/eligibility_ext.polar",
            |        "is_template": true
            |      }
            |    }
            |  ],
            |  "template_data": {
            |    "policy_extension": "eligibility_ext",
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
                    assert_ok!(PolicySource::from_template_file(
                        "./resources/eligibility.polar"
                    )),
                    assert_ok!(PolicySource::from_template_file(
                        "./resources/eligibility_ext.polar"
                    )),
                ],
                template_data: Some(EligibilityTemplateData {
                    policy_extension: Some("eligibility_ext".to_string()),
                    cooling_secs: Some(900),
                    stable_secs: Some(900),
                    custom: maplit::hashmap! { "foo".to_string() => "bar".to_string(), },
                }),
                ..EligibilitySettings::default()
            }
        )
    }
}
