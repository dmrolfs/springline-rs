use std::collections::HashSet;
use std::fmt::Debug;

use crate::metrics::UpdateMetrics;
use oso::PolarClass;
use pretty_snowflake::{Id, Label};
use proctor::elements::telemetry::UpdateMetricsFn;
use proctor::elements::{telemetry, Telemetry, Timestamp};
use proctor::error::DecisionError;
use proctor::phases::sense::SubscriptionRequirements;
use proctor::{Correlation, ProctorContext, SharedString};
use serde::{Deserialize, Serialize};

#[derive(PolarClass, Label, Debug, Clone, Serialize, Deserialize)]
pub struct DecisionContext {
    // auto-filled
    pub correlation_id: Id<Self>,
    pub recv_timestamp: Timestamp,

    #[polar(attribute)]
    #[serde(flatten)] // flatten enables sense of extra properties
    pub custom: telemetry::TableType,
}

impl Correlation for DecisionContext {
    type Correlated = Self;

    fn correlation(&self) -> &Id<Self::Correlated> {
        &self.correlation_id
    }
}

impl PartialEq for DecisionContext {
    fn eq(&self, other: &Self) -> bool {
        self.custom == other.custom
    }
}

impl SubscriptionRequirements for DecisionContext {
    fn required_fields() -> HashSet<proctor::SharedString> {
        HashSet::default()
    }
}

impl ProctorContext for DecisionContext {
    type Error = DecisionError;

    fn custom(&self) -> telemetry::TableType {
        self.custom.clone()
    }
}

impl UpdateMetrics for DecisionContext {
    fn update_metrics_for(phase_name: SharedString) -> UpdateMetricsFn {
        let update_fn = move |subscription_name: &str, telemetry: &Telemetry| match telemetry.clone().try_into::<Self>()
        {
            Ok(_ctx) => (),

            Err(err) => {
                tracing::warn!(error=?err, %phase_name, "failed to update decision context metrics on subscription: {}", subscription_name);
                proctor::track_errors(
                    phase_name.as_ref(),
                    &proctor::error::ProctorError::DecisionPhase(err.into()),
                );
            },
        };

        Box::new(update_fn)
    }
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
    fn test_serde_flink_decision_context_token() {
        let context = DecisionContext {
            correlation_id: Id::direct("DecisionContext", 0, "A"),
            recv_timestamp: Timestamp::new(0, 0),
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
            Token::Str("recv_timestamp"),
            Token::TupleStruct { name: "Timestamp", len: 2 },
            Token::I64(0),
            Token::U32(0),
            Token::TupleStructEnd,
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
            expected.swap(13, 15);
            expected.swap(14, 16);
            assert_tokens(&context, expected.as_slice());
        }
    }

    #[test]
    fn test_serde_flink_decision_context_from_telemetry() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);

        let data: Telemetry = maplit::hashmap! {
            "correlation_id" => Id::<DecisionContext>::direct("DecisionContext", 0, "A").to_telemetry(),
            "recv_timestamp" => Timestamp::new(0, 0).to_telemetry(),
            "foo" => "bar".to_telemetry(),
        }
        .into_iter()
        .collect();

        tracing::info!(telemetry=?data, "created telemetry");

        let actual = assert_ok!(data.try_into::<DecisionContext>());
        tracing::info!(?actual, "converted into FlinkDecisionContext");
        let expected = DecisionContext {
            correlation_id: Id::direct("DecisionContext", 0, "A"),
            recv_timestamp: Timestamp::new(0, 0),
            custom: maplit::hashmap! {"foo".to_string() => "bar".into(),},
        };
        tracing::info!("actual: {:?}", actual);
        tracing::info!("expected: {:?}", expected);
        assert_eq!(actual, expected);
    }
}
