use std::collections::HashSet;
use std::fmt::{self, Debug};

use oso::PolarClass;
use pretty_snowflake::Label;
use proctor::elements::telemetry::UpdateMetricsFn;
use proctor::elements::{telemetry, Telemetry};
use proctor::error::DecisionError;
use proctor::phases::sense::SubscriptionRequirements;
use proctor::ProctorContext;
use serde::{Deserialize, Serialize};

use crate::metrics::UpdateMetrics;

#[derive(PolarClass, Label, Clone, Serialize, Deserialize)]
pub struct DecisionContext {
    #[polar(attribute)]
    #[serde(flatten)] // flatten enables sense of extra properties
    pub custom: telemetry::TableType,
}

impl Debug for DecisionContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DecisionContext").field("custom", &self.custom).finish()
    }
}

impl PartialEq for DecisionContext {
    fn eq(&self, other: &Self) -> bool {
        self.custom == other.custom
    }
}

impl SubscriptionRequirements for DecisionContext {
    fn required_fields() -> HashSet<String> {
        HashSet::default()
    }
}

impl ProctorContext for DecisionContext {
    type ContextData = Self;
    type Error = DecisionError;

    fn custom(&self) -> telemetry::TableType {
        self.custom.clone()
    }
}

impl UpdateMetrics for DecisionContext {
    fn update_metrics_for(phase_name: &str) -> UpdateMetricsFn {
        let phase_name = phase_name.to_string();
        let update_fn = move |subscription_name: &str, telemetry: &Telemetry| match telemetry
            .clone()
            .try_into::<Self>()
        {
            Ok(_ctx) => (),

            Err(err) => {
                tracing::warn!(error=?err, %phase_name, "failed to update decision context metrics on subscription: {}", subscription_name);
                proctor::track_errors(
                    &phase_name,
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
            custom: maplit::hashmap! {
                "custom_foo".to_string() => "fred flintstone".into(),
                "custom_bar".to_string() => "The Happy Barber".into(),
            },
        };

        let mut expected = vec![
            Token::Map { len: None },
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
            expected.swap(1, 3);
            expected.swap(2, 4);
            assert_tokens(&context, expected.as_slice());
        }
    }

    #[test]
    fn test_serde_flink_decision_context_from_telemetry() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);

        let data: Telemetry = maplit::hashmap! {
            "foo" => "bar".to_telemetry(),
        }
        .into_iter()
        .collect();

        tracing::info!(telemetry=?data, "created telemetry");

        let actual = assert_ok!(data.try_into::<DecisionContext>());
        tracing::info!(?actual, "converted into FlinkDecisionContext");
        let expected = DecisionContext {
            custom: maplit::hashmap! {"foo".to_string() => "bar".into(),},
        };
        tracing::info!("actual: {:?}", actual);
        tracing::info!("expected: {:?}", expected);
        assert_eq!(actual, expected);
    }
}
