use std::collections::HashSet;
use std::fmt::Debug;

use oso::PolarClass;
use serde::{Deserialize, Serialize};

use proctor::elements::telemetry::{self, Table};
use proctor::error::GovernanceError;
use proctor::phases::collection::{Str, SubscriptionRequirements};
use proctor::ProctorContext;

#[derive(PolarClass, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GovernanceContext {
    #[polar(attribute)]
    pub min_cluster_size: u16,

    #[polar(attribute)]
    pub max_cluster_size: u16,

    #[polar(attribute)]
    pub max_scaling_step: u16,

    #[polar(attribute)]
    #[serde(flatten)]
    pub custom: telemetry::Table,
}

impl SubscriptionRequirements for GovernanceContext {
    fn required_fields() -> HashSet<Str> {
        maplit::hashset! {
            "min_cluster_size".into(),
            "max_cluster_size".into(),
            "max_scaling_step".into(),
        }
    }
}

impl ProctorContext for GovernanceContext {
    type Error = GovernanceError;

    fn custom(&self) -> Table {
        self.custom.clone()
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_test::{assert_tokens, Token};

    use super::*;
    use proctor::elements::telemetry::ToTelemetry;
    use proctor::elements::Telemetry;

    #[test]
    fn test_serde_flink_governance_context(// min_cluster_size in prop::num::u16::ANY,
            // max_cluster_size in prop::num::u16::ANY,
            // max_scaling_step in prop::num::u16::ANY,
            // custom_prop_a in prop::num::f64::ANY,
            // custom_prop_b in prop::num::i64::ANY,
    ) {
        let min_cluster_size = 7;
        let max_cluster_size = 199;
        let max_scaling_step = 33;
        let custom_prop_a = std::f64::consts::SQRT_2;
        let custom_prop_b = 242;

        let context = GovernanceContext {
            min_cluster_size,
            max_cluster_size,
            max_scaling_step,
            custom: maplit::hashmap! {
                "custom_prop_a".to_string() => custom_prop_a.into(),
                "custom_prop_b".to_string() => custom_prop_b.into(),
            },
        };

        let mut expected = vec![
            Token::Map { len: None },
            Token::Str("min_cluster_size"),
            Token::U16(min_cluster_size),
            Token::Str("max_cluster_size"),
            Token::U16(max_cluster_size),
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
            expected.swap(7, 9);
            expected.swap(8, 10);
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
        lazy_static::initialize(&proctor::tracing::TEST_TRACING);

        let min_cluster_size = 7;
        let max_cluster_size = 199;
        let max_scaling_step = 33;
        let foo = std::f64::consts::SQRT_2;
        let bar = 242;

        let data: Telemetry = maplit::hashmap! {
            "min_cluster_size" => min_cluster_size.to_telemetry(),
            "max_cluster_size" => max_cluster_size.to_telemetry(),
            "max_scaling_step" => max_scaling_step.to_telemetry(),
            "foo" => foo.to_telemetry(),
            "bar" => bar.to_telemetry(),
        }
        .into_iter()
        .collect();

        tracing::info!(telemetry=?data, "created telemetry");

        let actual = data.try_into::<GovernanceContext>();
        tracing::info!(?actual, "converted into FlinkGovernanceContext");
        let expected = GovernanceContext {
            min_cluster_size,
            max_cluster_size,
            max_scaling_step,
            custom: maplit::hashmap! {
                "foo".to_string() => foo.into(),
                "bar".to_string() => bar.into(),
            },
        };
        tracing::info!("actual: {:?}", actual);
        tracing::info!("expected: {:?}", expected);
        assert_eq!(actual.unwrap(), expected);
    }
}
