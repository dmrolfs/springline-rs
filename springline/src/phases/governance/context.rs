use std::collections::HashSet;
use std::fmt::{self, Debug};

use crate::flink::Parallelism;
use oso::PolarClass;
use pretty_snowflake::{Id, Label};
use proctor::elements::telemetry::UpdateMetricsFn;
use proctor::elements::{telemetry, Telemetry, Timestamp};
use proctor::error::GovernanceError;
use proctor::phases::sense::SubscriptionRequirements;
use proctor::{Correlation, ProctorContext};
use serde::{Deserialize, Serialize};

use crate::metrics::UpdateMetrics;
use crate::model::NrReplicas;

#[derive(PolarClass, Label, Clone, Serialize, Deserialize)]
pub struct GovernanceContext {
    // auto-filled
    pub correlation_id: Id<Self>,
    pub recv_timestamp: Timestamp,

    /// Minimal job parallelism autoscaling will allow to rescale to.
    /// - source from governance settings
    #[polar(attribute)]
    pub min_parallelism: Parallelism,

    /// Maximum job parallelism autoscaling will allow to rescale to.
    /// - source from governance settings
    #[polar(attribute)]
    pub max_parallelism: Parallelism,

    /// Minimal cluster size autoscaling will allow to rescale to.
    /// - source from governance settings
    #[polar(attribute)]
    pub min_cluster_size: NrReplicas,

    /// Maximum cluster size autoscaling will allow to rescale to.
    /// - source from governance settings
    #[polar(attribute)]
    pub max_cluster_size: NrReplicas,

    /// Minimal step in parallelism and cluster size autoscaling will allow to rescale.
    /// - source from governance settings
    #[polar(attribute)]
    pub min_scaling_step: u32,

    /// Maximum step in parallelism and cluster size autoscaling will allow to rescale.
    /// - source from governance settings
    #[polar(attribute)]
    pub max_scaling_step: u32,

    #[polar(attribute)]
    #[serde(flatten)]
    pub custom: telemetry::TableType,
}

impl Debug for GovernanceContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GovernanceContext")
            .field("correlation_id", &self.correlation_id)
            .field("recv_timestamp", &self.recv_timestamp.to_string())
            .field(
                "parallelism",
                &format!("[{}, {}]", self.min_parallelism, self.max_parallelism),
            )
            .field(
                "cluster_size",
                &format!("[{}, {}]", self.min_cluster_size, self.max_cluster_size),
            )
            .field(
                "scaling_step",
                &format!("[{}, {}]", self.min_scaling_step, self.max_scaling_step),
            )
            .field("custom", &self.custom)
            .finish()
    }
}

impl GovernanceContext {
    #[inline]
    pub fn cluster_size_in_bounds(&self, cluster_size: NrReplicas) -> NrReplicas {
        let above_min = NrReplicas::max(self.min_cluster_size, cluster_size);
        let result = NrReplicas::min(above_min, self.max_cluster_size);
        tracing::warn!(
            ?self,
            "DMR: cluster_size_in_bounds({cluster_size}) = {result}"
        );
        result
    }

    #[inline]
    pub fn parallelism_in_bounds(&self, parallelism: Parallelism) -> Parallelism {
        let above_min = Parallelism::max(self.min_parallelism, parallelism);
        let result = Parallelism::min(above_min, self.max_parallelism);
        tracing::warn!(
            ?self,
            "DMR: parallelism_in_bounds({parallelism}) = {result}"
        );
        result
    }
}

impl Correlation for GovernanceContext {
    type Correlated = Self;

    fn correlation(&self) -> &Id<Self::Correlated> {
        &self.correlation_id
    }
}

impl PartialEq for GovernanceContext {
    fn eq(&self, other: &Self) -> bool {
        self.min_parallelism == other.min_parallelism
            && self.max_parallelism == other.max_parallelism
            && self.min_cluster_size == other.min_cluster_size
            && self.max_cluster_size == other.max_cluster_size
            && self.min_scaling_step == other.min_scaling_step
            && self.max_scaling_step == other.max_scaling_step
            && self.custom == other.custom
    }
}

impl SubscriptionRequirements for GovernanceContext {
    fn required_fields() -> HashSet<String> {
        maplit::hashset! {
            "min_parallelism".into(),
            "max_parallelism".into(),
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
    fn update_metrics_for(phase_name: &str) -> UpdateMetricsFn {
        let phase_name = phase_name.to_string();
        let update_fn = move |subscription_name: &str, telemetry: &Telemetry| match telemetry
            .clone()
            .try_into::<Self>()
        {
            Ok(_ctx) => {
                // GOVERNANCE_CTX_MIN_CLUSTER_SIZE.set(ctx.min_cluster_size as i64);
                // GOVERNANCE_CTX_MAX_CLUSTER_SIZE.set(ctx.max_cluster_size as i64);
                // GOVERNANCE_CTX_MAX_SCALING_STEP.set(ctx.max_scaling_step as i64);
            },

            Err(err) => {
                tracing::warn!(error=?err, %phase_name, "failed to update governance context metrics on subscription: {}", subscription_name);
                proctor::track_errors(
                    &phase_name,
                    &proctor::error::ProctorError::GovernancePhase(err.into()),
                );
            },
        };

        Box::new(update_fn)
    }
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
    fn test_serde_flink_governance_context_token(// min_cluster_size in prop::num::u16::ANY,
            // max_cluster_size in prop::num::u16::ANY,
            // max_scaling_step in prop::num::u16::ANY,
            // custom_prop_a in prop::num::f64::ANY,
            // custom_prop_b in prop::num::i64::ANY,
    ) {
        let min_parallelism = Parallelism::new(5);
        let max_parallelism = Parallelism::new(250);
        let min_cluster_size = NrReplicas::new(7);
        let max_cluster_size = NrReplicas::new(199);
        let min_scaling_step = 1;
        let max_scaling_step = 33;
        let custom_prop_a = std::f64::consts::SQRT_2;
        let custom_prop_b = 242;

        let context = GovernanceContext {
            correlation_id: Id::direct("GovernanceContext", 0, "A"),
            recv_timestamp: Timestamp::new(0, 0),
            min_parallelism,
            max_parallelism,
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
            Token::Str("recv_timestamp"),
            Token::TupleStruct { name: "Timestamp", len: 2 },
            Token::I64(0),
            Token::U32(0),
            Token::TupleStructEnd,
            Token::Str("min_parallelism"),
            Token::U32(min_parallelism.into()),
            Token::Str("max_parallelism"),
            Token::U32(max_parallelism.into()),
            Token::Str("min_cluster_size"),
            Token::U32(min_cluster_size.into()),
            Token::Str("max_cluster_size"),
            Token::U32(max_cluster_size.into()),
            Token::Str("min_scaling_step"),
            Token::U32(min_scaling_step),
            Token::Str("max_scaling_step"),
            Token::U32(max_scaling_step),
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
            expected.swap(25, 27);
            expected.swap(26, 28);
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

        let min_parallelism = Parallelism::new(3);
        let max_parallelism = Parallelism::new(300);
        let min_cluster_size = NrReplicas::new(7);
        let max_cluster_size = NrReplicas::new(199);
        let min_scaling_step = 2;
        let max_scaling_step = 33;
        let foo = std::f64::consts::SQRT_2;
        let bar = 242;

        let data: Telemetry = maplit::hashmap! {
            "correlation_id" => Id::<GovernanceContext>::direct("GovernanceContext", 0, "A").to_telemetry(),
            "recv_timestamp" => Timestamp::new(0, 0).to_telemetry(),
            "min_parallelism" => min_parallelism.to_telemetry(),
            "max_parallelism" => max_parallelism.to_telemetry(),
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
            recv_timestamp: Timestamp::new(0, 0),
            min_parallelism,
            max_parallelism,
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
