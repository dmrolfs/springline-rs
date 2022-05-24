use std::collections::HashSet;
use std::fmt::{self, Debug};

use chrono::{DateTime, Utc};
use frunk::{Monoid, Semigroup};
use once_cell::sync::Lazy;
use oso::PolarClass;
use pretty_snowflake::{Id, Label, Labeling};
use proctor::elements::telemetry::UpdateMetricsFn;
use proctor::elements::{telemetry, Telemetry, Timestamp};
use proctor::error::{EligibilityError, ProctorError};
use proctor::phases::sense::SubscriptionRequirements;
use proctor::{Correlation, ProctorContext};
use prometheus::IntGauge;
use serde::{Deserialize, Serialize};

use crate::metrics::UpdateMetrics;

#[derive(PolarClass, Label, Clone, Serialize, Deserialize)]
pub struct EligibilityContext {
    // auto-filled
    pub correlation_id: Id<Self>,
    pub recv_timestamp: Timestamp,

    #[polar(attribute)]
    #[serde(flatten)] // current subscription mechanism only supports flatten keys
    pub task_status: TaskStatus,

    #[polar(attribute)]
    #[serde(flatten)] // current subscription mechanism only supports flatten keys
    pub cluster_status: ClusterStatus,

    #[polar(attribute)]
    pub all_sinks_healthy: bool,

    #[polar(attribute)]
    #[serde(flatten)] // flatten to collect extra properties.
    pub custom: telemetry::TableType,
}

impl Debug for EligibilityContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EligibilityContext")
            .field("correlation_id", &self.correlation_id)
            .field("recv_timestamp", &self.recv_timestamp.to_string())
            .field("task_status", &self.task_status)
            .field("cluster_status", &self.cluster_status)
            .field("all_sinks_healthy", &self.all_sinks_healthy)
            .field("custom", &self.custom)
            .finish()
    }
}
impl Correlation for EligibilityContext {
    type Correlated = Self;

    fn correlation(&self) -> &Id<Self::Correlated> {
        &self.correlation_id
    }
}

impl Monoid for EligibilityContext {
    fn empty() -> Self {
        Self {
            correlation_id: Id::direct(<Self as Label>::labeler().label(), 0, "<undefined>"),
            recv_timestamp: Timestamp::ZERO,
            task_status: TaskStatus::empty(),
            cluster_status: ClusterStatus::empty(),
            all_sinks_healthy: true,
            custom: telemetry::TableType::new(),
        }
    }
}

impl Semigroup for EligibilityContext {
    fn combine(&self, other: &Self) -> Self {
        let mut custom = self.custom.clone();
        custom.extend(other.custom.clone());

        Self {
            correlation_id: other.correlation_id.clone(),
            recv_timestamp: other.recv_timestamp,
            task_status: self.task_status.combine(&other.task_status),
            cluster_status: self.cluster_status.combine(&other.cluster_status),
            all_sinks_healthy: other.all_sinks_healthy,
            custom,
        }
    }
}

impl PartialEq for EligibilityContext {
    fn eq(&self, other: &Self) -> bool {
        self.task_status == other.task_status
            && self.cluster_status == other.cluster_status
            && self.custom == other.custom
    }
}

impl SubscriptionRequirements for EligibilityContext {
    fn required_fields() -> HashSet<String> {
        maplit::hashset! {
            // this works because we rename the property via #serde field attributes
        }
    }

    fn optional_fields() -> HashSet<String> {
        maplit::hashset! {
            "all_sinks_healthy".into(),
            "task.last_failure".into(),
            CLUSTER__IS_RESCALING.into(),
            "cluster.is_deploying".into(),
            CLUSTER__LAST_DEPLOYMENT.into(),
        }
    }
}

impl ProctorContext for EligibilityContext {
    type Error = EligibilityError;

    fn custom(&self) -> telemetry::TableType {
        self.custom.clone()
    }
}

impl UpdateMetrics for EligibilityContext {
    fn update_metrics_for(phase_name: &str) -> UpdateMetricsFn {
        let phase_name = phase_name.to_string();
        let update_fn = move |subscription_name: &str, telemetry: &Telemetry| match telemetry.clone().try_into::<Self>()
        {
            Ok(ctx) => {
                if let Some(last_failure_ts) = ctx.task_status.last_failure.map(|ts| ts.timestamp()) {
                    ELIGIBILITY_CTX_TASK_LAST_FAILURE.set(last_failure_ts);
                }

                ELIGIBILITY_CTX_ALL_SINKS_HEALTHY.set(ctx.all_sinks_healthy as i64);
                ELIGIBILITY_CTX_CLUSTER_IS_DEPLOYING.set(ctx.cluster_status.is_deploying as i64);
                ELIGIBILITY_CTX_CLUSTER_LAST_DEPLOYMENT.set(ctx.cluster_status.last_deployment.timestamp());
            },

            Err(err) => {
                tracing::warn!(
                    error=?err, %phase_name, ?telemetry,
                    "failed to update eligibility context metrics on subscription: {}", subscription_name
                );
                proctor::track_errors(&phase_name, &ProctorError::EligibilityPhase(err.into()));
            },
        };

        Box::new(update_fn)
    }
}

#[derive(PolarClass, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskStatus {
    // todo: I don't I can get this from Flink - maybe from deployment or k8s?
    // todo: remove struct in favor of metric_catalog's job health uptime -
    // probably not since that metric doesn't work properly under reactive mode.
    #[serde(default)]
    #[serde(
        rename = "task.last_failure",
        serialize_with = "proctor::serde::date::serialize_optional_datetime_map",
        deserialize_with = "proctor::serde::date::deserialize_optional_datetime"
    )]
    pub last_failure: Option<DateTime<Utc>>,
}

impl TaskStatus {
    pub fn last_failure_within_seconds(&self, seconds: i64) -> bool {
        self.last_failure.map_or(false, |last_failure| {
            let boundary = Utc::now() - chrono::Duration::seconds(seconds);
            boundary < last_failure
        })
    }
}

impl Monoid for TaskStatus {
    fn empty() -> Self {
        Self { last_failure: None }
    }
}

impl Semigroup for TaskStatus {
    fn combine(&self, other: &Self) -> Self {
        let last_failure = match (&self.last_failure, &other.last_failure) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };

        Self { last_failure: last_failure.cloned() }
    }
}

pub const CLUSTER__LAST_DEPLOYMENT: &str = "cluster.last_deployment";
pub const CLUSTER__IS_RESCALING: &str = "cluster.is_rescaling";

#[derive(PolarClass, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterStatus {
    // todo: options to source property:
    // query k8s to describe job-manager pod in json and filter for .status.conditions
    // is_deploying is false until type:Ready has status:True
    // - poll Flink readiness probe; i.e., k8s pod status
    // - poll an ENVVAR or a cvs file; init-container can update accordingly
    #[polar(attribute)]
    #[serde(rename = "cluster.is_deploying")]
    pub is_deploying: bool,

    /// Springline is actively rescaling the cluster. This status is supplied as feedback from the
    /// Action phase.
    #[polar(attribute)]
    #[serde(default, rename = "cluster.is_rescaling")]
    pub is_rescaling: bool,

    // todo: source property via k8s describe job-manager pod in json then
    // filter for .status.conditions until type:Ready has status:True
    // then last_deployment is lastTransitionTime; e.g., "2021-11-22T04:28:07Z"
    #[serde(with = "proctor::serde", rename = "cluster.last_deployment")]
    pub last_deployment: DateTime<Utc>,
}

impl ClusterStatus {
    pub fn last_deployment_within_seconds(&self, seconds: i64) -> bool {
        let boundary = Utc::now() - chrono::Duration::seconds(seconds);
        boundary < self.last_deployment
    }
}

impl Monoid for ClusterStatus {
    fn empty() -> Self {
        Self {
            is_deploying: false,
            is_rescaling: false,
            last_deployment: Timestamp::new(0, 0).as_utc(),
        }
    }
}

impl Semigroup for ClusterStatus {
    fn combine(&self, other: &Self) -> Self {
        Self {
            is_deploying: other.is_deploying,
            is_rescaling: other.is_rescaling,
            last_deployment: self.last_deployment.max(other.last_deployment),
        }
    }
}

pub static ELIGIBILITY_CTX_ALL_SINKS_HEALTHY: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "eligibility_ctx_all_sinks_healthy",
        "Are all sinks for the FLink jobs healthy",
    )
    .expect("failed creating eligibility_ctx_all_sinks_healthy metric")
});

pub static ELIGIBILITY_CTX_TASK_LAST_FAILURE: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "eligibility_ctx_task_last_failure",
        "UNIX timestamp in seconds of last Flink Task Manager failure in environment",
    )
    .expect("failed creating eligibility_ctx_task_last_failure metric")
});

pub static ELIGIBILITY_CTX_CLUSTER_IS_DEPLOYING: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "eligibility_ctx_cluster_is_deploying",
        "Is the Flink cluster actively deploying: 1=yes, 0=no",
    )
    .expect("failed creating eligibility_ctx_cluster_is_deploying metric")
});

pub static ELIGIBILITY_CTX_CLUSTER_LAST_DEPLOYMENT: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "eligibility_ctx_cluster_last_deployment",
        "UNIX timestamp in seconds of last deployment of the Flink cluster",
    )
    .expect("failed creating eligibility_ctx_cluster_last_deployment metric")
});
