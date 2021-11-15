use std::collections::HashSet;
use std::fmt::Debug;

use lazy_static::lazy_static;
use oso::PolarClass;
use pretty_snowflake::{Id, Label};
use proctor::elements::{telemetry, Telemetry, Timestamp};
use proctor::error::DecisionError;
use proctor::phases::collection::SubscriptionRequirements;
use proctor::{ProctorContext, SharedString};
use prometheus::IntGauge;
use serde::{Deserialize, Serialize};

use crate::phases::UpdateMetrics;

#[derive(PolarClass, Label, Debug, Clone, Serialize, Deserialize)]
pub struct DecisionContext {
    // auto-filled
    pub correlation_id: Id<Self>,
    pub timestamp: Timestamp,

    #[polar(attribute)]
    pub all_sinks_healthy: bool,

    #[polar(attribute)]
    #[serde(rename = "cluster.nr_task_managers")]
    pub nr_task_managers: u16,

    #[polar(attribute)]
    #[serde(flatten)] // flatten enables collection of extra properties
    pub custom: telemetry::TableType,
}

impl PartialEq for DecisionContext {
    fn eq(&self, other: &Self) -> bool {
        self.all_sinks_healthy == other.all_sinks_healthy
            && self.nr_task_managers == other.nr_task_managers
            && self.custom == self.custom
    }
}

impl SubscriptionRequirements for DecisionContext {
    fn required_fields() -> HashSet<proctor::SharedString> {
        maplit::hashset! {
            "all_sinks_healthy".into(),
            "cluster.nr_task_managers".into(),
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
            },

            Err(err) => {
                tracing::warn!(error=?err, %phase_name, "failed to update decision context metrics on subscription: {}", subscription_name);
                proctor::track_errors(
                    phase_name.as_ref(),
                    &proctor::error::ProctorError::DecisionError(err.into()),
                );
            },
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
    use claim::*;
    use pretty_assertions::assert_eq;
    use proctor::elements::telemetry::ToTelemetry;
    use proctor::elements::Telemetry;
    use serde_test::{assert_tokens, Token};

    use super::*;

    #[test]
    fn test_serde_flink_decision_context() {
        let context = DecisionContext {
            correlation_id: Id::direct("DecisionContext", 0, "A"),
            timestamp: Timestamp::new(0, 0),
            all_sinks_healthy: true,
            nr_task_managers: 4,
            custom: maplit::hashmap! {
                "custom_foo".to_string() => "fred flintstone".into(),
                "custom_bar".to_string() => "The Happy Barber".into(),
            },
        };

        let mut expected = vec![
            Token::Map { len: None },
            Token::Str("correlation_id"),
            Token::Struct { name: "Id", len: 2 },
            Token::Str("snowflake"),
            Token::I64(0),
            Token::Str("pretty"),
            Token::Str("A"),
            Token::StructEnd,
            Token::Str("timestamp"),
            Token::TupleStruct { name: "Timestamp", len: 2 },
            Token::I64(0),
            Token::U32(0),
            Token::TupleStructEnd,
            Token::Str("all_sinks_healthy"),
            Token::Bool(true),
            Token::Str("cluster.nr_task_managers"),
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
            expected.swap(17, 19);
            expected.swap(18, 20);
            assert_tokens(&context, expected.as_slice());
        }
    }

    #[test]
    fn test_serde_flink_decision_context_from_telemetry() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);

        let data: Telemetry = maplit::hashmap! {
            "correlation_id" => Id::<DecisionContext>::direct("DecisionContext", 0, "A").to_telemetry(),
            "timestamp" => Timestamp::new(0, 0).to_telemetry(),
            "all_sinks_healthy" => false.to_telemetry(),
            "cluster.nr_task_managers" => 4.to_telemetry(),
            "foo" => "bar".to_telemetry(),
        }
        .into_iter()
        .collect();

        tracing::info!(telemetry=?data, "created telemetry");

        let actual = assert_ok!(data.try_into::<DecisionContext>());
        tracing::info!(?actual, "converted into FlinkDecisionContext");
        let expected = DecisionContext {
            correlation_id: Id::direct("DecisionContext", 0, "A"),
            timestamp: Timestamp::new(0, 0),
            all_sinks_healthy: false,
            nr_task_managers: 4,
            custom: maplit::hashmap! {"foo".to_string() => "bar".into(),},
        };
        tracing::info!("actual: {:?}", actual);
        tracing::info!("expected: {:?}", expected);
        assert_eq!(actual, expected);
    }
}
