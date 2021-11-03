use oso::{Oso, PolarClass, PolarValue};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

use super::context::DecisionContext;
use crate::phases::decision::result::DECISION_BINDING;
use crate::phases::{MetricCatalog, UpdateMetrics};
use proctor::elements::{PolicySettings, PolicySource, PolicySubscription, QueryPolicy, QueryResult, Telemetry};
use proctor::error::PolicyError;
use proctor::phases::collection::TelemetrySubscription;
use proctor::{ProctorContext, SharedString};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct DecisionTemplateData {
    pub basis: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_healthy_lag: Option<f64>,
    pub min_healthy_lag: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_healthy_cpu_load: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_healthy_network_io: Option<f64>,
    #[serde(flatten, skip_serializing_if = "HashMap::is_empty")]
    pub custom: HashMap<String, String>,
}

impl Default for DecisionTemplateData {
    fn default() -> Self {
        Self {
            basis: format!("{}_basis", DecisionPolicy::base_template_name()),
            max_healthy_lag: None,
            min_healthy_lag: 0_f64,
            max_healthy_cpu_load: None,
            max_healthy_network_io: None,
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
    pub fn new(settings: &PolicySettings<DecisionTemplateData>) -> Self {
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
    type Item = MetricCatalog;
    type Context = DecisionContext;
    type Args = (Self::Item, Self::Context, PolarValue);
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

    fn initialize_policy_engine(&mut self, engine: &mut Oso) -> Result<(), PolicyError> {
        Telemetry::initialize_policy_engine(engine)?;

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
            PolarValue::Variable(DECISION_BINDING.to_string()),
        )
    }

    fn query_policy(&self, engine: &Oso, args: Self::Args) -> Result<QueryResult, PolicyError> {
        QueryResult::from_query(engine.query_rule("scale", args)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use claim::*;
    use pretty_assertions::assert_eq;
    use proctor::elements::PolicyRegistry;
    use serde_test::{assert_tokens, Token};
    use trim_margin::MarginTrimmable;

    #[test]
    fn test_decision_data_serde() {
        let data = DecisionTemplateData {
            basis: "decision_basis".to_string(),
            max_healthy_lag: Some(133_f64),
            min_healthy_lag: 1_f64,
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

        let actual: PolicySettings<DecisionTemplateData> = assert_ok!(c.try_into());
        assert_eq!(
            actual,
            PolicySettings {
                template_data: Some(data),
                ..PolicySettings::default()
            }
        );
    }

    #[test]
    fn test_template() {
        lazy_static::initialize(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_template");
        let _ = main_span.enter();

        let data = Some(DecisionTemplateData {
            custom: maplit::hashmap! {
                "max_records_in_per_sec".to_string() => 3_f64.to_string(),
                "min_records_in_per_sec".to_string() => 1_f64.to_string(),
            },
            ..DecisionTemplateData::default()
        });
        let template = r##"
            | scale_up(item, _context, _) if {{max_records_in_per_sec}} < item.flow.records_in_per_sec;
            | scale_down(item, _context, _) if item.flow.records_in_per_sec < {{min_records_in_per_sec}};
        "##
        .trim_margin_with("|")
        .unwrap();

        let reg = PolicyRegistry::new();
        let json_data = serde_json::json!(data);
        tracing::info!(%json_data, ?data, "json test data");
        assert_eq!(
            json_data.to_string(),
            r##"{"basis":"decision_basis","max_records_in_per_sec":"3","min_healthy_lag":0.0,"min_records_in_per_sec":"1"}"##.to_string()
        );

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
        lazy_static::initialize(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_policy_composition");
        let _ = main_span.enter();

        let sources = vec![
            PolicySource::from_template_file("./resources/decision.polar")?,
            PolicySource::from_template_string(
                format!("{}_basis", DecisionPolicy::base_template_name()),
                r###"
                |{{> preamble}}
                |scale_up(item, _context, _) if {{max_records_in_per_sec}} < item.flow.records_in_per_sec;
                |scale_down(item, _context, _) if item.flow.records_in_per_sec < {{min_records_in_per_sec}};
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
        let settings = PolicySettings::<DecisionTemplateData> {
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
        let expected = r##"
        |scale(item, context, direction) if scale_up(item, context, direction) and direction = "up";
        |
        |scale(item, context, direction) if scale_down(item, context, direction) and direction = "down";
        |scale_up(item, _context, _) if 3 < item.flow.records_in_per_sec;
        |scale_down(item, _context, _) if item.flow.records_in_per_sec < 1;
        "##
        .trim_margin_with("|")
        .unwrap();
        assert_eq!(actual, expected);
        Ok(())
    }
}
