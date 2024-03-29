use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug};
use std::time::Duration;

use crate::flink::MC_CLUSTER__FREE_TASK_SLOTS;
use crate::phases::decision::DECISION_DIRECTION;
use crate::phases::plan::{ForecastInputs, ScaleDirection};
use once_cell::sync::Lazy;
use pretty_snowflake::Label;
use proctor::phases::sense::SubscriptionRequirements;
use prometheus::core::{AtomicU64, GenericGauge, GenericGaugeVec};
use prometheus::Opts;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

pub const PLANNING__TOTAL_TASK_SLOTS: &str = "cluster.total_task_slots";
pub const PLANNING__FREE_TASK_SLOTS: &str = MC_CLUSTER__FREE_TASK_SLOTS;
pub const PLANNING__RESCALE_RESTART: &str = "planning.rescale_restart_secs";
pub const PLANNING__MAX_CATCH_UP: &str = "planning.max_catch_up_secs";
pub const PLANNING__RECOVERY_VALID: &str = "planning.recovery_valid_secs";

pub const DIRECTION: &str = DECISION_DIRECTION;
pub const DURATION_SECS: &str = "duration_secs";

#[serde_as]
#[derive(Label, Clone, Serialize, Deserialize)]
pub struct PlanningContext {
    /// Allowed cluster size change in a rescaling action.
    #[serde(default, rename = "planning.min_scaling_step")]
    pub min_scaling_step: Option<u32>,

    /// Observed total task slots.
    #[serde(
        default = "PlanningContext::default_total_task_slots",
        rename = "cluster.total_task_slots"
    )]
    pub total_task_slots: u32,

    /// Observed free task slots.
    #[serde(
        default = "PlanningContext::default_total_task_slots",
        rename = "cluster.free_task_slots"
    )]
    pub free_task_slots: u32,

    /// Time expected to restart Flink when scaling. Baseline time is set via configuration, but as
    /// springline rescales, it measures the restart duration and updates planning accordingly.
    #[serde(default, rename = "planning.rescale_restart_secs")]
    #[serde_as(
        as = "HashMap<serde_with::DisplayFromStr, serde_with::DurationSecondsWithFrac<f64>>"
    )]
    pub rescale_restart: HashMap<ScaleDirection, Duration>,

    /// Configured maximum time allowed to catch up processing accumulated records after the
    /// rescaling action. If the tolerating a longer catch-up time, allows the target cluster size
    /// to closely match that required for the predicted target workload. A shorter allowed catch
    /// up time may result in over-provisioning the cluster.
    #[serde(default, rename = "planning.max_catch_up_secs")]
    #[serde_as(as = "Option<serde_with::DurationSeconds<u64>>")]
    pub max_catch_up: Option<Duration>,

    /// The time after recovery allowed to settle the cluster.
    #[serde(default, rename = "planning.recovery_valid_secs")]
    #[serde_as(as = "Option<serde_with::DurationSeconds<u64>>")]
    pub recovery_valid: Option<Duration>,
}

impl Debug for PlanningContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlanningContext")
            .field("min_scaling_step", &self.min_scaling_step)
            .field("total_task_slots", &self.total_task_slots)
            .field("free_task_slots", &self.free_task_slots)
            .field("rescale_restart", &self.rescale_restart)
            .field("max_catch_up", &self.max_catch_up)
            .field("recovery_valid", &self.recovery_valid)
            .finish()
    }
}

impl PlanningContext {
    pub const fn default_total_task_slots() -> u32 {
        0
    }

    #[tracing::instrument(level = "trace")]
    pub fn patch_inputs(&self, inputs: &mut ForecastInputs) {
        if !self.rescale_restart.is_empty() {
            tracing::info!(rescale_restart=?self.rescale_restart, old_restart_input=?inputs.direction_restart, "patching planning rescale_restart");
            inputs.direction_restart.extend(self.rescale_restart.clone());
        }

        if let Some(c) = self.max_catch_up {
            tracing::info!(max_catch_up=?c, old_max_catch_up_input=?inputs.max_catch_up, "patching planning max_catch_up");
            inputs.max_catch_up = c;
        }

        if let Some(valid) = self.recovery_valid {
            tracing::info!(?valid, old_valid_offset_input=?inputs.valid_offset, "patching planning valid_offset");
            inputs.valid_offset = valid;
        }
    }
}

impl PartialEq for PlanningContext {
    fn eq(&self, other: &Self) -> bool {
        self.min_scaling_step == other.min_scaling_step
            && self.total_task_slots == other.total_task_slots
            && self.free_task_slots == other.free_task_slots
            && self.rescale_restart == other.rescale_restart
            && self.max_catch_up == other.max_catch_up
            && self.recovery_valid == other.recovery_valid
    }
}

impl SubscriptionRequirements for PlanningContext {
    fn required_fields() -> HashSet<String> {
        HashSet::default()
    }

    fn optional_fields() -> HashSet<String> {
        maplit::hashset! {
            "planning.min_scaling_step".into(),
            PLANNING__TOTAL_TASK_SLOTS.into(),
            PLANNING__FREE_TASK_SLOTS.into(),
            PLANNING__RESCALE_RESTART.into(),
            "planning.max_catch_up_secs".into(),
            "planning.recovery_valid_secs".into(),
        }
    }
}

pub static PLANNING_CTX_MIN_SCALING_STEP: Lazy<GenericGauge<AtomicU64>> = Lazy::new(|| {
    GenericGauge::with_opts(
        Opts::new(
            "planning_ctx_min_scaling_step",
            "Minimum step when rescaling the cluster",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating planning_ctx_min_scaling_step metric")
});

pub static PLANNING_CTX_FORECASTING_RESTART_SECS: Lazy<GenericGaugeVec<AtomicU64>> =
    Lazy::new(|| {
        GenericGaugeVec::new(
            Opts::new(
                "planning_ctx_forecasting_restart_secs",
                "expected rescale up restart duration in secs used for forecasting",
            )
            .const_labels(proctor::metrics::CONST_LABELS.clone()),
            &["direction"],
        )
        .expect("failed creating planning_ctx_forecasting_up_restart_secs metric")
    });

pub static PLANNING_CTX_FORECASTING_MAX_CATCH_UP_SECS: Lazy<GenericGauge<AtomicU64>> =
    Lazy::new(|| {
        GenericGauge::with_opts(
            Opts::new(
                "planning_ctx_forecasting_max_catch_up_secs",
                "expected max catch-up duration in secs used for forecasting",
            )
            .const_labels(proctor::metrics::CONST_LABELS.clone()),
        )
        .expect("failed creating planning_ctx_forecasting_max_catch_up_secs metric")
    });

pub static PLANNING_CTX_FORECASTING_RECOVERY_VALID_SECS: Lazy<GenericGauge<AtomicU64>> =
    Lazy::new(|| {
        GenericGauge::with_opts(
            Opts::new(
                "planning_ctx_forecasting_recovery_valid_secs",
                "expected duration in secs until the recovery is valid - used for forecasting",
            )
            .const_labels(proctor::metrics::CONST_LABELS.clone()),
        )
        .expect("failed creating planning_ctx_forecasting_recovery_valid_secs metric")
    });

#[cfg(test)]
mod tests {
    use serde_test::{assert_tokens, Token};

    use super::*;

    #[test]
    fn test_planning_context_serde_tokens() {
        let context = PlanningContext {
            total_task_slots: 1,
            free_task_slots: 0,
            min_scaling_step: None,
            rescale_restart: maplit::hashmap! {
                ScaleDirection::Down =>Duration::from_secs(23),
            },
            max_catch_up: None,
            recovery_valid: Some(Duration::from_secs(22)),
        };

        assert_tokens(
            &context,
            &vec![
                Token::Struct { name: "PlanningContext", len: 6 },
                Token::Str("planning.min_scaling_step"),
                Token::None,
                Token::Str("cluster.total_task_slots"),
                Token::U32(1),
                Token::Str("cluster.free_task_slots"),
                Token::U32(0),
                Token::Str("planning.rescale_restart_secs"),
                Token::Map { len: Some(1) },
                Token::Str("down"),
                Token::F64(23_f64),
                Token::MapEnd,
                Token::Str("planning.max_catch_up_secs"),
                Token::None,
                Token::Str("planning.recovery_valid_secs"),
                Token::Some,
                Token::U64(22),
                Token::StructEnd,
            ],
        );
    }
}
