use std::collections::HashSet;
use std::fmt::Debug;

use oso::PolarClass;
use serde::{Deserialize, Serialize};

use proctor::elements::telemetry;
use proctor::phases::collection::SubscriptionRequirements;
use proctor::ProctorContext;

#[derive(PolarClass, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FlinkDecisionContext {
    #[polar(attribute)]
    pub all_sinks_healthy: bool,

    #[polar(attribute)]
    pub nr_task_managers: u16,

    #[polar(attribute)]
    #[serde(flatten)] // flatten enables collection of extra properties
    pub custom: telemetry::Table,
}

impl SubscriptionRequirements for FlinkDecisionContext {
    fn required_fields() -> HashSet<&'static str> {
        maplit::hashset! {
            "all_sinks_healthy",
            "nr_task_managers",
        }
    }
}

impl ProctorContext for FlinkDecisionContext {
    fn custom(&self) -> telemetry::Table {
        self.custom.clone()
    }
}

// /////////////////////////////////////////////////////
// // Unit Tests ///////////////////////////////////////

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_test::{assert_tokens, Token};

    use super::*;
    use proctor::elements::telemetry::ToTelemetry;
    use proctor::elements::Telemetry;

    #[test]
    fn test_serde_flink_decision_context() {
        let context = FlinkDecisionContext {
            all_sinks_healthy: true,
            nr_task_managers: 4,
            custom: maplit::hashmap! {
                "custom_foo".to_string() => "fred flintstone".into(),
                "custom_bar".to_string() => "The Happy Barber".into(),
            },
        };

        let mut expected = vec![
            Token::Map { len: None },
            Token::Str("all_sinks_healthy"),
            Token::Bool(true),
            Token::Str("nr_task_managers"),
            Token::U16(4),
            Token::Str("custom_foo"),
            Token::Str("fred flintstone"),
            Token::Str("custom_bar"),
            Token::Str("The Happy Barber"),
            Token::MapEnd,
        ];

        let result = std::panic::catch_unwind(|| {
            assert_tokens(&context, expected.as_slice());
        });

        if result.is_err() {
            expected.swap(5, 7);
            expected.swap(6, 8);
            assert_tokens(&context, expected.as_slice());
        }
    }

    #[test]
    fn test_serde_flink_decision_context_from_telemetry() {
        lazy_static::initialize(&proctor::tracing::TEST_TRACING);

        let data: Telemetry = maplit::hashmap! {
            "all_sinks_healthy" => false.to_telemetry(),
            "nr_task_managers" => 4.to_telemetry(),
            "foo" => "bar".to_telemetry(),
        }
        .into_iter()
        .collect();

        tracing::info!(telemetry=?data, "created telemetry");

        let actual = data.try_into::<FlinkDecisionContext>();
        tracing::info!(?actual, "converted into FlinkDecisionContext");
        let expected = FlinkDecisionContext {
            all_sinks_healthy: false,
            nr_task_managers: 4,
            custom: maplit::hashmap! {"foo".to_string() => "bar".into(),},
        };
        tracing::info!("actual: {:?}", actual);
        tracing::info!("expected: {:?}", expected);
        assert_eq!(actual.unwrap(), expected);
    }
}
