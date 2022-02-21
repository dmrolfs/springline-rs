use crate::phases::UpdateMetrics;
use once_cell::sync::Lazy;
use pretty_snowflake::{Id, Label};
use proctor::elements::telemetry::UpdateMetricsFn;
use proctor::elements::{Telemetry, Timestamp};
use proctor::error::ProctorError;
use proctor::phases::sense::SubscriptionRequirements;
use proctor::SharedString;
use prometheus::IntGauge;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::collections::HashSet;
use std::time::Duration;

#[serde_as]
#[derive(Label, Debug, Clone, Serialize, Deserialize)]
pub struct PlanningContext {
    // auto-filled
    pub correlation_id: Id<Self>,
    pub recv_timestamp: Timestamp,

    /// Allowed cluster size change in a rescaling action.
    #[serde(rename = "planning.min_scaling_step")]
    pub min_scaling_step: Option<u32>,

    /// Time expected to restart Flink when scaling. Baseline time is set via configuration, but as
    /// springline rescales, it measures the restart duration and updates planning accordingly.
    #[serde(rename = "planning.restart")]
    #[serde_as(as = "Option<serde_with::DurationSeconds<u64>>")]
    pub restart: Option<Duration>,

    /// Configured maximum time allowed to catch up processing accumulated records after the
    /// rescaling action. If the tolerating a longer catch-up time, allows the target cluster size
    /// to closely match that required for the predicted target workload. A shorter allowed catch
    /// up time may result in over-provisioning the cluster.
    #[serde(rename = "planning.max_catch_up")]
    #[serde_as(as = "Option<serde_with::DurationSeconds<u64>>")]
    pub max_catch_up: Option<Duration>,

    /// The time after recovery allowed to settle the cluster.
    #[serde(rename = "planning.recovery_valid")]
    #[serde_as(as = "Option<serde_with::DurationSeconds<u64>>")]
    pub recovery_valid: Option<Duration>,
}

impl PartialEq for PlanningContext {
    fn eq(&self, other: &Self) -> bool {
        self.min_scaling_step == other.min_scaling_step
            && self.restart == other.restart
            && self.max_catch_up == other.max_catch_up
            && self.recovery_valid == other.recovery_valid
    }
}

impl SubscriptionRequirements for PlanningContext {
    fn required_fields() -> HashSet<SharedString> {
        HashSet::default()
    }

    fn optional_fields() -> HashSet<SharedString> {
        maplit::hashset! {
            "planning.min_scaling_step".into(),
            "planning.restart".into(),
            "planning.max_catch_up".into(),
            "planning.recovery_valid".into(),
        }
    }
}

impl UpdateMetrics for PlanningContext {
    fn update_metrics_for(phase_name: SharedString) -> UpdateMetricsFn {
        let update_fn = move |subscription_name: &str, telemetry: &Telemetry| match telemetry.clone().try_into::<Self>()
        {
            Ok(ctx) => {
                PLANNING_CTX_MIN_SCALING_STEP.set(ctx.min_scaling_step as i64);
                PLANNING_CTX_FORECASTING_RESTART_SECS.set(ctx.restart.as_secs() as i64);
                PLANNING_CTX_FORECASTING_MAX_CATCH_UP_SECS.set(ctx.max_catch_up.as_secs() as i64);
                PLANNING_CTX_FORECASTING_RECOVERY_VALID_SECS.set(ctx.recovery_valid.as_secs() as i64);
            },

            Err(err) => {
                tracing::warn!(
                    error=?err, %phase_name,
                    "failed to update eligibility context metrics on subscription: {}", subscription_name
                );
                proctor::track_errors(phase_name.as_ref(), &ProctorError::PlanPhase(err.into()));
            },
        };

        Box::new(update_fn)
    }
}

pub static PLANNING_CTX_MIN_SCALING_STEP: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "planning_ctx_min_scaling_step",
        "Minimum step when rescaling the cluster",
    )
    .expect("failed creating planning_ctx_min_scaling_step metric")
});

pub static PLANNING_CTX_FORECASTING_RESTART_SECS: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "planning_ctx_forecasting_restart_secs",
        "expected restart duration in secs used for forecasting",
    )
    .expect("failed creating planning_ctx_forecasting_restart_secs metric")
});

pub static PLANNING_CTX_FORECASTING_MAX_CATCH_UP_SECS: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "planning_ctx_forecasting_max_catch_up_secs",
        "expected max catch-up duration in secs used for forecasting",
    )
    .expect("failed creating planning_ctx_forecasting_max_catch_up_secs metric")
});

pub static PLANNING_CTX_FORECASTING_RECOVERY_VALID_SECS: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "planning_ctx_forecasting_recovery_valid_secs",
        "expected duration in secs until the recovery is valid - used for forecasting",
    )
    .expect("failed creating planning_ctx_forecasting_recovery_valid_secs metric")
});

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_test::{assert_tokens, Token};
    use super::*;

    fn test_planning_context_serde_tokens() {
        let now = Timestamp::now();
        let corr = Id::direct("PlanningContext", "ABC");
        let context = PlanningContext {
            recv_timestamp: now,
            correlation_id: corr.clone(),
            min_scaling_step: None,
            restart: Some(Duration::from_secs(17)),
            max_catch_up: None,
            recovery_valid:Some(Duration::from_secs(22)),
        };

        assert_tokens(
            &context,
            &vec![]
        );
    }
}