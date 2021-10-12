use std::collections::HashSet;
use std::fmt::Debug;

use oso::PolarClass;
use serde::{Deserialize, Serialize};

use crate::phases::UpdateMetrics;
use lazy_static::lazy_static;
use proctor::elements::{telemetry, Telemetry};
use proctor::error::DecisionError;
use proctor::phases::collection::SubscriptionRequirements;
use proctor::{ProctorContext, SharedString};
use prometheus::IntGauge;

#[derive(PolarClass, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecisionContext {
    #[polar(attribute)]
    pub all_sinks_healthy: bool,

    #[polar(attribute)]
    pub nr_task_managers: u16,

    #[polar(attribute)]
    #[serde(flatten)] // flatten enables collection of extra properties
    pub custom: telemetry::TableType,
}

impl SubscriptionRequirements for DecisionContext {
    fn required_fields() -> HashSet<proctor::SharedString> {
        maplit::hashset! {
            "all_sinks_healthy".into(),
            "nr_task_managers".into(),
        }
    }
}

impl ProctorContext for DecisionContext {
    type Error = DecisionError;

    fn custom(&self) -> telemetry::TableType {
        self.custom.clone()
    }
}

impl UpdateMetrics for DecisionContext {
    fn update_metrics_for(phase_name: SharedString) -> Box<dyn Fn(&str, &Telemetry) -> () + Send + Sync + 'static> {
        let update_fn = move |subscription_name: &str, telemetry: &Telemetry| match telemetry
            .clone()
            .try_into::<DecisionContext>()
        {
            Ok(ctx) => {
                DECISION_CTX_ALL_SINKS_HEALTHY.set(ctx.all_sinks_healthy as i64);
                DECISION_CTX_NR_TASK_MANAGERS.set(ctx.nr_task_managers as i64);
            }

            Err(err) => {
                tracing::warn!(error=?err, %phase_name, "failed to update decision context metrics on subscription: {}", subscription_name);
                proctor::track_errors(
                    phase_name.as_ref(),
                    &proctor::error::ProctorError::DecisionError(err.into()),
                );
            }
        };

        Box::new(update_fn)
    }
}

lazy_static! {
    pub(crate) static ref DECISION_CTX_ALL_SINKS_HEALTHY: IntGauge = IntGauge::new(
        "decision_ctx_all_sinks_healthy",
        "Are all sinks for the FLink jobs healthy"
    )
    .expect("failed creating decision_ctx_all_sinks_healthy");
    pub(crate) static ref DECISION_CTX_NR_TASK_MANAGERS: IntGauge =
        IntGauge::new("decision_ctx_nr_task_managers", "Number of active Flink Task Managers")
            .expect("failed creating decision_ctx_nr_task_managers");
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
        let context = DecisionContext {
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

        let actual = data.try_into::<DecisionContext>();
        tracing::info!(?actual, "converted into FlinkDecisionContext");
        let expected = DecisionContext {
            all_sinks_healthy: false,
            nr_task_managers: 4,
            custom: maplit::hashmap! {"foo".to_string() => "bar".into(),},
        };
        tracing::info!("actual: {:?}", actual);
        tracing::info!("expected: {:?}", expected);
        assert_eq!(actual.unwrap(), expected);
    }
}
