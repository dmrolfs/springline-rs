use std::collections::{HashMap, HashSet};

use crate::Env;
use once_cell::sync::Lazy;
use oso::{Oso, PolarClass, PolarValue};
use proctor::elements::{
    telemetry, PolicyContributor, PolicySource, PolicySubscription, QueryPolicy, QueryResult,
    Telemetry,
};
use proctor::error::PolicyError;
use proctor::phases::sense::TelemetrySubscription;
use proctor::ProctorContext;
use prometheus::{IntCounterVec, Opts};
use serde::{Deserialize, Serialize};
use validator::Validate;

use super::context::DecisionContext;
use crate::flink::AppDataWindow;
use crate::metrics::UpdateMetrics;
use crate::phases::decision::result::DECISION_DIRECTION;
use crate::phases::decision::{DecisionData, DecisionDataT};
use crate::phases::REASON;
use crate::settings::DecisionSettings;

#[derive(Debug, Clone, Validate, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct DecisionTemplateData {
    pub basis: String,

    /// Optional threshold representing the maximum ratio the
    /// `total_lag` rate / `consumed_records_rate` that is healthy.
    /// This value represents how much the workload is increasing (positive)
    /// or decreasing (negative). This value is used for scale up decisions
    /// and not scale down decisions.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(min = 0.0))]
    pub max_healthy_relative_lag_velocity: Option<f64>,

    /// Optional threshold representing the maximum `total_lag`
    /// that is healthy. This factor does not consider the workload,
    /// so its usefulness may be limited compared to the
    /// `max_healthy_relative_lag_velocity`. This could be used to
    /// define a cap to `total_lag` as a supplemental rule to the
    /// relative lag velocity.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_healthy_lag: Option<u32>,

    /// Optional threshold on the target task utilization, which is
    /// calculated based on the `idle_time_millis_per_sec`. This value
    /// is used for scale down decisions and is typically coupled with
    /// a check that the `total_lag` is 0 -- meaning the application
    /// is caught up on workload and underutilized.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(min = 0.0))]
    pub min_task_utilization: Option<f64>,

    /// Optional threshold that may scale up the application if the CPU load
    /// is too high. High resource utilization (CPU, Memory, Network)) may suggest
    /// the application is destablizing, which scaling up may help.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(min = 0.0))]
    pub max_healthy_cpu_load: Option<f64>,

    /// Optional threshold that may scale up the application if the heap memory load
    /// is too high. High resource utilization (CPU, Memory, Network) may suggest
    /// the application is destablizing, which scaling up may help.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(min = 0.0))]
    pub max_healthy_heap_memory_load: Option<f64>,

    /// Optional threshold that may scale up the application if network IO
    /// is too high. High resource utilization (CPU, Memory, Network) may suggest
    /// the application is destablizing, which scaling up may help.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(min = 0.0))]
    pub max_healthy_network_io_utilization: Option<f64>,

    /// Optional threshold, among possibly others, that may indicate an application is idle. If this value
    /// or min_task_utilization are not set, then rescale down for low utilization and idle telemetry is
    /// disabled.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(min = 0.0, max = 1000.0))]
    pub min_idle_source_back_pressured_time_millis_per_sec: Option<f64>,

    /// Many of the rules look at the rolling average of the values to reduce the
    /// affects of short-term spikes. This optional value sets the common evaluation
    /// window for these range metric forms. Duration may range from 0 (always single item) to 600
    /// (10 minutes).
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(min = 0, max = 600))]
    pub evaluate_duration_secs: Option<u32>,

    /// Custom template values are collected in the custom property.
    #[serde(flatten, skip_serializing_if = "HashMap::is_empty")]
    pub custom: HashMap<String, String>,
}

impl Default for DecisionTemplateData {
    fn default() -> Self {
        Self {
            basis: format!("{}_basis", DecisionPolicy::base_template_name()),
            max_healthy_relative_lag_velocity: None,
            max_healthy_lag: None,
            min_task_utilization: None,
            max_healthy_cpu_load: None,
            max_healthy_heap_memory_load: None,
            max_healthy_network_io_utilization: None,
            min_idle_source_back_pressured_time_millis_per_sec: None,
            evaluate_duration_secs: None,
            custom: HashMap::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DecisionPolicy {
    pub required_subscription_fields: HashSet<String>,
    pub optional_subscription_fields: HashSet<String>,
    pub sources: Vec<PolicySource>,
    pub template_data: Option<DecisionTemplateData>,
}

impl DecisionPolicy {
    pub fn new(settings: &DecisionSettings) -> Result<Self, PolicyError> {
        let mut template_data = settings.template_data.clone();
        if let Some(td) = &mut template_data {
            td.validate()?;
        }

        Ok(Self {
            required_subscription_fields: settings.required_subscription_fields.clone(),
            optional_subscription_fields: settings.optional_subscription_fields.clone(),
            sources: settings.policies.clone(),
            template_data: settings.template_data.clone(),
        })
    }
}

impl PolicySubscription for DecisionPolicy {
    type Requirements = DecisionContext;

    fn do_extend_subscription(&self, subscription: TelemetrySubscription) -> TelemetrySubscription {
        let update_fn = Self::Requirements::update_metrics_for(subscription.name());
        subscription.with_update_metrics_fn(update_fn)
    }
}

impl QueryPolicy for DecisionPolicy {
    type Args = (DecisionDataT, DecisionContext, PolarValue, PolarValue);
    type Context = Env<DecisionContext>;
    type Item = DecisionData;
    type TemplateData = DecisionTemplateData;

    fn base_template_name() -> &'static str {
        "decision"
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

    fn initialize_policy_engine(&self, engine: &mut Oso) -> Result<(), PolicyError> {
        Telemetry::register_with_policy_engine(engine)?;
        AppDataWindow::register_with_policy_engine(engine)?;

        engine.register_class(
            DecisionContext::get_polar_class_builder()
                .add_method("custom", ProctorContext::custom)
                .build(),
        )?;

        Ok(())
    }

    fn make_query_args(&self, item: &Self::Item, context: &Self::Context) -> Self::Args {
        (
            item.as_ref().clone(),
            context.as_ref().clone(),
            PolarValue::Variable(DECISION_DIRECTION.to_string()),
            PolarValue::Variable(REASON.to_string()),
        )
    }

    fn query_policy(&self, engine: &Oso, args: Self::Args) -> Result<QueryResult, PolicyError> {
        QueryResult::from_query(engine.query_rule("scale", args)?)
    }

    fn zero_context(&self) -> Option<Self::Context> {
        if !self.required_subscription_fields.is_empty() {
            None
        } else {
            Some(Env::new(DecisionContext {
                custom: telemetry::TableType::default(),
            }))
        }
    }
}

pub static DECISION_SCALING_DECISION_COUNT_METRIC: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "decision_scaling_decision_count",
            "Count of decisions for scaling planning made.",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
        &["decision", "reason"],
    )
    .expect("failed creating decision_scaling_decision_count metric")
});

#[cfg(test)]
mod tests {
    use claim::*;
    use pretty_assertions::assert_eq;
    use proctor::elements::PolicyRegistry;
    use serde_test::{assert_tokens, Token};
    use trim_margin::MarginTrimmable;

    use super::*;

    #[test]
    fn test_decision_data_serde() {
        let data = DecisionTemplateData {
            basis: "decision_basis".to_string(),
            max_healthy_relative_lag_velocity: Some(-1.25),
            max_healthy_lag: Some(133),
            max_healthy_cpu_load: Some(0.7),
            ..DecisionTemplateData::default()
        };

        assert_tokens(
            &data,
            &vec![
                Token::Map { len: None },
                Token::Str("basis"),
                Token::Str("decision_basis"),
                Token::Str("max_healthy_relative_lag_velocity"),
                Token::Some,
                Token::F64(-1.25_f64),
                Token::Str("max_healthy_lag"),
                Token::Some,
                Token::U32(133),
                Token::Str("max_healthy_cpu_load"),
                Token::Some,
                Token::F64(0.7),
                Token::MapEnd,
            ],
        );

        let c = assert_ok!(config::Config::builder()
            .add_source(config::File::from(std::path::PathBuf::from(
                "./tests/data/decision.ron"
            )))
            .build());

        let actual: DecisionSettings = assert_ok!(c.try_deserialize());
        assert_eq!(
            actual,
            DecisionSettings {
                template_data: Some(data),
                ..DecisionSettings::default()
            }
        );
    }

    #[test]
    fn test_decision_data_ron_serde() {
        let data = DecisionTemplateData {
            basis: "decision_basis".to_string(),
            max_healthy_lag: Some(133),
            max_healthy_relative_lag_velocity: Some(1_f64),
            max_healthy_cpu_load: Some(0.7),
            custom: maplit::hashmap! { "foo".to_string() => "zed".to_string(), },
            ..DecisionTemplateData::default()
        };

        let rep = assert_ok!(ron::ser::to_string_pretty(
            &data,
            ron::ser::PrettyConfig::default()
        ));
        let expected_rep = r##"|{
        |    "basis": "decision_basis",
        |    "max_healthy_relative_lag_velocity": Some(1.0),
        |    "max_healthy_lag": Some(133),
        |    "max_healthy_cpu_load": Some(0.7),
        |    "foo": "zed",
        |}"##
            .trim_margin_with("|")
            .unwrap();
        assert_eq!(rep, expected_rep);

        let mut ron_deser = assert_ok!(ron::Deserializer::from_str(&rep));
        let json_rep = vec![];
        let mut json_ser = serde_json::Serializer::new(json_rep);
        assert_ok!(serde_transcode::transcode(&mut ron_deser, &mut json_ser));
        let json_rep = assert_ok!(String::from_utf8(json_ser.into_inner()));

        let actual: DecisionTemplateData = assert_ok!(serde_json::from_str(&json_rep));
        assert_eq!(actual, data);
    }

    #[test]
    fn test_template() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_template");
        let _ = main_span.enter();

        let data = Some(DecisionTemplateData {
            custom: maplit::hashmap! {
                "max_records_in_per_sec".to_string() => 3_f64.to_string(),
                "min_records_in_per_sec".to_string() => 1_f64.to_string(),
            },
            ..DecisionTemplateData::default()
        });

        let data_rep = assert_ok!(serde_json::to_string_pretty(&data));
        if let Err(err) = std::panic::catch_unwind(|| {
            assert_eq!(
                data_rep,
                r##"|{
            |  "basis": "decision_basis",
            |  "max_records_in_per_sec": "3",
            |  "min_records_in_per_sec": "1"
            |}"##
                    .trim_margin_with("|")
                    .unwrap()
                    .to_string()
            )
        }) {
            tracing::error!(error=?err, "first option failed - trying element switch...");
            assert_eq!(
                data_rep,
                r##"|{
            |  "basis": "decision_basis",
            |  "min_records_in_per_sec": "1",
            |  "max_records_in_per_sec": "3"
            |}"##
                    .trim_margin_with("|")
                    .unwrap()
                    .to_string()
            );
        }

        let template = r##"
            | scale_up(item, _context, _) if {{max_records_in_per_sec}} < item.flow.records_in_per_sec;
            | scale_down(item, _context, _) if item.flow.records_in_per_sec < {{min_records_in_per_sec}};
        "##
        .trim_margin_with("|")
        .unwrap();

        let reg = PolicyRegistry::new();
        let json_data = serde_json::json!(data);
        tracing::info!(%json_data, ?data, "json test data");
        let actual = assert_ok!(reg.render_template(&template, &json_data));

        let expected = r##"
            | scale_up(item, _context, _) if 3 < item.flow.records_in_per_sec;
            | scale_down(item, _context, _) if item.flow.records_in_per_sec < 1;
        "##
        .trim_margin_with("|")
        .unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_policy_composition() -> anyhow::Result<()> {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_policy_composition");
        let _ = main_span.enter();

        let sources = vec![
            PolicySource::from_template_file("../resources/decision.polar")?,
            PolicySource::from_template_string(
                format!("{}_basis", DecisionPolicy::base_template_name()),
                r###"|scale_up(item, _context, reason) if {{max_records_in_per_sec}} < item.flow.records_in_per_sec and reason = "load_up";
                     |scale_down(item, _context, reason) if item.flow.records_in_per_sec < {{min_records_in_per_sec}} and reason = "load_down";
                "###,
            )?,
        ];

        let name = DecisionPolicy::base_template_name();
        let template_data = Some(DecisionTemplateData {
            custom: maplit::hashmap! {
                "max_records_in_per_sec".to_string() => 3.to_string(),
                "min_records_in_per_sec".to_string() => 1.to_string(),
            },
            ..DecisionTemplateData::default()
        });
        let settings = DecisionSettings {
            required_subscription_fields: HashSet::default(),
            optional_subscription_fields: HashSet::default(),
            policies: sources,
            template_data,
        };
        let policy = assert_ok!(DecisionPolicy::new(&settings));

        let actual = proctor::elements::policy_filter::render_template_policy(
            policy.sources(),
            name,
            settings.template_data.as_ref(),
        )?;
        // let actual = policy_filter::render_template_policy(name, settings.template_data.as_ref())?;
        let expected =
            r##"|scale(item, context, direction, reason) if scale_up(item, context, reason) and direction = "up";
                |
                |scale(item, context, direction, reason) if scale_down(item, context, reason) and direction = "down";
                |
                |evaluation_window(window) if window = 60;
                |
                |min_utilization(utilization) if utilization = 0.25;
                |
                |idle_source_telemetry(item) if
                |  item.flow.source_total_lag == nil
                |  or item.flow.source_records_lag_max == nil
                |  or item.flow.source_assigned_partitions == nil;
                |
                |lag_increasing(item, velocity) if
                |  evaluation_window(window)
                |  and not idle_source_telemetry(item)
                |  and velocity = item.flow_source_relative_lag_velocity(window)
                |  and 0.0 < velocity;
                |
                |lag_decreasing(item, velocity) if
                |  evaluation_window(window)
                |  and not idle_source_telemetry(item)
                |  and velocity = item.flow_source_relative_lag_velocity(window)
                |  and velocity < 0.0;
                |
                |scale_up(item, _context, reason) if 3 < item.flow.records_in_per_sec and reason = "load_up";
                |scale_down(item, _context, reason) if item.flow.records_in_per_sec < 1 and reason = "load_down";
                |
                |# no action rules to avoid policy issues if corresponding up/down rules not specified in basis.polar
                |scale_up(_, _, _) if false;
                |scale_down(_, _, _) if false;
                |"##
            .trim_margin_with("|")
            .unwrap();
        assert_eq!(actual, expected);
        Ok(())
    }
}
