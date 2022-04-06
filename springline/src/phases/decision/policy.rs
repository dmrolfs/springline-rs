use once_cell::sync::Lazy;
use std::collections::{HashMap, HashSet};

use crate::model::MetricPortfolio;
use oso::{Oso, PolarClass, PolarValue};
use proctor::elements::{
    PolicyContributor, PolicySource, PolicySubscription, QueryPolicy, QueryResult, Telemetry, Timestamp,
};
use proctor::error::PolicyError;
use proctor::phases::sense::TelemetrySubscription;
use proctor::{ProctorContext, ProctorIdGenerator, SharedString};
use prometheus::{IntCounterVec, Opts};
use serde::{Deserialize, Serialize};

use super::context::DecisionContext;
use crate::metrics::UpdateMetrics;
use crate::phases::decision::result::DECISION_DIRECTION;
use crate::phases::REASON;
use crate::settings::DecisionSettings;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct DecisionTemplateData {
    pub basis: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_healthy_lag: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_healthy_lag: Option<f64>,

    // todo: use lag for both kafka and kinesis -- units obviously different but uses are mutually exclusive?
    // #[serde(skip_serializing_if = "Option::is_none")]
    // pub max_healthy_millis_behind_latest: Option<f64>,
    // #[serde(skip_serializing_if = "Option::is_none")]
    // pub min_healthy_millis_behind_latest: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_healthy_cpu_load: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_healthy_cpu_load: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_healthy_heap_memory_load: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_healthy_network_io_utilization: Option<f64>,

    #[serde(flatten, skip_serializing_if = "HashMap::is_empty")]
    pub custom: HashMap<String, String>,
}

impl Default for DecisionTemplateData {
    fn default() -> Self {
        Self {
            basis: format!("{}_basis", DecisionPolicy::base_template_name()),
            max_healthy_lag: None,
            min_healthy_lag: None,
            max_healthy_cpu_load: None,
            min_healthy_cpu_load: None,
            max_healthy_heap_memory_load: None,
            max_healthy_network_io_utilization: None,
            custom: HashMap::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DecisionPolicy {
    pub required_subscription_fields: HashSet<SharedString>,
    pub optional_subscription_fields: HashSet<SharedString>,
    pub sources: Vec<PolicySource>,
    pub template_data: Option<DecisionTemplateData>,
}

impl DecisionPolicy {
    pub fn new(settings: &DecisionSettings) -> Self {
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

impl PolicySubscription for DecisionPolicy {
    type Requirements = DecisionContext;

    fn do_extend_subscription(&self, subscription: TelemetrySubscription) -> TelemetrySubscription {
        let update_fn = Self::Requirements::update_metrics_for(subscription.name());
        subscription.with_update_metrics_fn(update_fn)
    }
}

impl QueryPolicy for DecisionPolicy {
    type Args = (Self::Item, Self::Context, PolarValue, PolarValue);
    type Context = DecisionContext;
    type Item = MetricPortfolio;
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
        MetricPortfolio::register_with_policy_engine(engine)?;

        engine.register_class(
            DecisionContext::get_polar_class_builder()
                .add_method("custom", ProctorContext::custom)
                .build(),
        )?;

        Ok(())
    }

    fn make_query_args(&self, item: &Self::Item, context: &Self::Context) -> Self::Args {
        (
            item.clone(),
            context.clone(),
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
            // todo: context corr-id is generally created in clearinghouse, but this is special case,
            // until consideration about how to pull in generator settings, use this to start.
            let mut gen: ProctorIdGenerator<DecisionContext> = ProctorIdGenerator::default();

            Some(Self::Context {
                recv_timestamp: Timestamp::now(),
                correlation_id: gen.next_id(),
                custom: HashMap::default(),
            })
        }
    }
}

pub static DECISION_SCALING_DECISION_COUNT_METRIC: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "decision_scaling_decision_count",
            "Count of decisions for scaling planning made.",
        ),
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
            max_healthy_lag: Some(133_f64),
            min_healthy_lag: Some(1_f64),
            max_healthy_cpu_load: Some(0.7),
            ..DecisionTemplateData::default()
        };

        assert_tokens(
            &data,
            &vec![
                Token::Map { len: None },
                Token::Str("basis"),
                Token::Str("decision_basis"),
                Token::Str("max_healthy_lag"),
                Token::Some,
                Token::F64(133_f64),
                Token::Str("min_healthy_lag"),
                Token::Some,
                Token::F64(1_f64),
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

        let actual: DecisionSettings = assert_ok!(c.try_into());
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
            max_healthy_lag: Some(133_f64),
            min_healthy_lag: Some(1_f64),
            max_healthy_cpu_load: Some(0.7),
            custom: maplit::hashmap! { "foo".to_string() => "zed".to_string(), },
            ..DecisionTemplateData::default()
        };

        let rep = assert_ok!(ron::ser::to_string_pretty(&data, ron::ser::PrettyConfig::default()));
        let expected_rep = r##"|{
        |    "basis": "decision_basis",
        |    "max_healthy_lag": Some(133),
        |    "min_healthy_lag": Some(1),
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
                r###"
                |{{> preamble}}
                |scale_up(item, _context, _, reason) if {{max_records_in_per_sec}} < item.flow.records_in_per_sec and reason = "load_up";
                |scale_down(item, _context, _, reason) if item.flow.records_in_per_sec < {{min_records_in_per_sec}} and reason = "load_down";
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
        let policy = DecisionPolicy::new(&settings);

        let actual = proctor::elements::policy_filter::render_template_policy(
            policy.sources(),
            name,
            settings.template_data.as_ref(),
        )?;
        // let actual = policy_filter::render_template_policy(name, settings.template_data.as_ref())?;
        let expected = r##"|
        |scale(item, context, direction, reason) if scale_up(item, context, direction, reason) and direction = "up";
        |
        |scale(item, context, direction, reason) if scale_down(item, context, direction, reason) and direction = "down";
        |scale_up(item, _context, _, reason) if 3 < item.flow.records_in_per_sec and reason = "load_up";
        |scale_down(item, _context, _, reason) if item.flow.records_in_per_sec < 1 and reason = "load_down";
        |
        |# no action rules to avoid policy errors if corresponding up/down rules not specified in basis.polar
        |scale_up(_, _, _, _) if false;
        |scale_down(_, _, _, _) if false;
        "##
        .trim_margin_with("|")
        .unwrap();
        assert_eq!(actual, expected);
        Ok(())
    }
}
