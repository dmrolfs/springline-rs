use std::collections::HashSet;
use std::fmt::Debug;

use lazy_static::lazy_static;
use oso::PolarClass;
use pretty_snowflake::{Id, Label};
use proctor::elements::{telemetry, Telemetry, Timestamp};
use proctor::error::GovernanceError;
use proctor::phases::collection::SubscriptionRequirements;
use proctor::{ProctorContext, SharedString};
use prometheus::IntGauge;
use serde::{Deserialize, Serialize};

use crate::phases::UpdateMetrics;

#[derive(PolarClass, Label, Debug, Clone, Serialize, Deserialize)]
pub struct GovernanceContext {
    // auto-filled
    pub correlation_id: Id<Self>,
    pub timestamp: Timestamp,

    #[polar(attribute)]
    pub min_cluster_size: u16,

    #[polar(attribute)]
    pub max_cluster_size: u16,

    #[polar(attribute)]
    pub min_scaling_step: u16,

    #[polar(attribute)]
    pub max_scaling_step: u16,

    #[polar(attribute)]
    #[serde(flatten)]
    pub custom: telemetry::TableType,
}

impl PartialEq for GovernanceContext {
    fn eq(&self, other: &Self) -> bool {
        self.min_cluster_size == other.min_cluster_size
            && self.max_cluster_size == other.max_cluster_size
            && self.min_scaling_step == other.min_scaling_step
            && self.max_scaling_step == other.max_scaling_step
            && self.custom == other.custom
    }
}

impl SubscriptionRequirements for GovernanceContext {
    fn required_fields() -> HashSet<proctor::SharedString> {
        maplit::hashset! {
            "min_cluster_size".into(),
            "max_cluster_size".into(),
            "min_scaling_step".into(),
            "max_scaling_step".into(),
        }
    }
}

impl ProctorContext for GovernanceContext {
    type Error = GovernanceError;

    fn custom(&self) -> telemetry::TableType {
        self.custom.clone()
    }
}

impl UpdateMetrics for GovernanceContext {
    fn update_metrics_for(phase_name: SharedString) -> Box<dyn Fn(&str, &Telemetry) -> () + Send + Sync + 'static> {
        let update_fn = move |subscription_name: &str, telemetry: &Telemetry| match telemetry
            .clone()
            .try_into::<GovernanceContext>()
        {
            Ok(ctx) => {
                GOVERNANCE_CTX_MIN_CLUSTER_SIZE.set(ctx.min_cluster_size as i64);
                GOVERNANCE_CTX_MAX_CLUSTER_SIZE.set(ctx.max_cluster_size as i64);
                GOVERNANCE_CTX_MAX_SCALING_STEP.set(ctx.max_scaling_step as i64);
            },

            Err(err) => {
                tracing::warn!(error=?err, %phase_name, "failed to update governance context metrics on subscription: {}", subscription_name);
                proctor::track_errors(
                    phase_name.as_ref(),
                    &proctor::error::ProctorError::GovernanceError(err.into()),
                );
            },
        };

        Box::new(update_fn)
    }
}

lazy_static! {
    pub(crate) static ref GOVERNANCE_CTX_MIN_CLUSTER_SIZE: IntGauge =
        IntGauge::new("governance_ctx_min_cluster_size", "Minimum cluster size allowed",)
            .expect("failed creating governance_ctx_min_cluster_size metric");
    pub(crate) static ref GOVERNANCE_CTX_MAX_CLUSTER_SIZE: IntGauge =
        IntGauge::new("governance_ctx_max_cluster_size", "Maximum cluster size allowed",)
            .expect("failed creating governance_ctx_max_cluster_size metric");
    pub(crate) static ref GOVERNANCE_CTX_MAX_SCALING_STEP: IntGauge = IntGauge::new(
        "governance_ctx_max_scaling_step",
        "Maximum change in cluster size allowed.",
    )
    .expect("failed creating governance_ctx_max_scaling_step metric");
}

#[cfg(test)]
mod tests {
    use claim::*;
    use pretty_assertions::assert_eq;
    use proctor::elements::telemetry::ToTelemetry;
    use proctor::elements::Telemetry;
    use serde_test::{assert_tokens, Token};

    use super::*;

    #[test]
    fn test_serde_flink_governance_context(// min_cluster_size in prop::num::u16::ANY,
            // max_cluster_size in prop::num::u16::ANY,
            // max_scaling_step in prop::num::u16::ANY,
            // custom_prop_a in prop::num::f64::ANY,
            // custom_prop_b in prop::num::i64::ANY,
    ) {
        let min_cluster_size = 7;
        let max_cluster_size = 199;
        let min_scaling_step = 1;
        let max_scaling_step = 33;
        let custom_prop_a = std::f64::consts::SQRT_2;
        let custom_prop_b = 242;

        let context = GovernanceContext {
            correlation_id: Id::direct("GovernanceContext", 0, "A"),
            timestamp: Timestamp::new(0, 0),
            min_cluster_size,
            max_cluster_size,
            min_scaling_step,
            max_scaling_step,
            custom: maplit::hashmap! {
                "custom_prop_a".to_string() => custom_prop_a.into(),
                "custom_prop_b".to_string() => custom_prop_b.into(),
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
            Token::Str("min_cluster_size"),
            Token::U16(min_cluster_size),
            Token::Str("max_cluster_size"),
            Token::U16(max_cluster_size),
            Token::Str("min_scaling_step"),
            Token::U16(min_scaling_step),
            Token::Str("max_scaling_step"),
            Token::U16(max_scaling_step),
            Token::Str("custom_prop_a"),
            Token::F64(custom_prop_a),
            Token::Str("custom_prop_b"),
            Token::I64(custom_prop_b),
            Token::MapEnd,
        ];

        let result = std::panic::catch_unwind(|| {
            assert_tokens(&context, expected.as_slice());
        });

        if result.is_err() {
            expected.swap(21, 23);
            expected.swap(22, 24);
            assert_tokens(&context, expected.as_slice());
        }
    }

    #[test]
    fn test_serde_flink_governance_context_from_telemetry(// min_cluster_size in prop::num::u16::ANY,
            // max_cluster_size in prop::num::u16::ANY,
            // max_scaling_step in prop::num::u16::ANY,
            // foo in prop::num::f64::ANY,
            // bar in prop::num::i64::ANY,
    ) {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);

        let min_cluster_size = 7;
        let max_cluster_size = 199;
        let min_scaling_step = 2;
        let max_scaling_step = 33;
        let foo = std::f64::consts::SQRT_2;
        let bar = 242;

        let data: Telemetry = maplit::hashmap! {
            "correlation_id" => Id::<GovernanceContext>::direct("GovernanceContext", 0, "A").to_telemetry(),
            "timestamp" => Timestamp::new(0, 0).to_telemetry(),
            "min_cluster_size" => min_cluster_size.to_telemetry(),
            "max_cluster_size" => max_cluster_size.to_telemetry(),
            "min_scaling_step" => min_scaling_step.to_telemetry(),
            "max_scaling_step" => max_scaling_step.to_telemetry(),
            "foo" => foo.to_telemetry(),
            "bar" => bar.to_telemetry(),
        }
        .into_iter()
        .collect();

        tracing::info!(telemetry=?data, "created telemetry");

        let actual = assert_ok!(data.try_into::<GovernanceContext>());
        tracing::info!(?actual, "converted into FlinkGovernanceContext");
        let expected = GovernanceContext {
            correlation_id: Id::direct("GovernanceContext", 0, "A"),
            timestamp: Timestamp::new(0, 0),
            min_cluster_size,
            max_cluster_size,
            min_scaling_step,
            max_scaling_step,
            custom: maplit::hashmap! {
                "foo".to_string() => foo.into(),
                "bar".to_string() => bar.into(),
            },
        };
        tracing::info!("actual: {:?}", actual);
        tracing::info!("expected: {:?}", expected);
        assert_eq!(actual, expected);
    }
}
