use std::collections::HashSet;
use std::fmt::Debug;

use chrono::{DateTime, Utc};
use once_cell::sync::Lazy;
use oso::PolarClass;
use pretty_snowflake::{Id, Label};
use proctor::elements::telemetry::UpdateMetricsFn;
use proctor::elements::{telemetry, Telemetry, Timestamp};
use proctor::error::{EligibilityError, ProctorError};
use proctor::phases::collection::SubscriptionRequirements;
use proctor::{ProctorContext, SharedString};
use prometheus::IntGauge;
use serde::{Deserialize, Serialize};

use crate::phases::UpdateMetrics;

#[derive(PolarClass, Label, Debug, Clone, Serialize, Deserialize)]
pub struct EligibilityContext {
    // auto-filled
    pub correlation_id: Id<Self>,
    pub timestamp: Timestamp,

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

impl PartialEq for EligibilityContext {
    fn eq(&self, other: &Self) -> bool {
        self.task_status == other.task_status
            && self.cluster_status == other.cluster_status
            && self.custom == other.custom
    }
}

impl SubscriptionRequirements for EligibilityContext {
    fn required_fields() -> HashSet<proctor::SharedString> {
        maplit::hashset! {
            // this works because we rename the property via #serde field attributes
            "cluster.is_deploying".into(),
            "cluster.last_deployment".into(),
            "all_sinks_healthy".into(),
        }
    }

    fn optional_fields() -> HashSet<SharedString> {
        maplit::hashset! {
            "task.last_failure".into(),
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
    fn update_metrics_for(phase_name: SharedString) -> UpdateMetricsFn {
        let update_fn = move |subscription_name: &str, telemetry: &Telemetry| match telemetry
            .clone()
            .try_into::<EligibilityContext>()
        {
            Ok(ctx) => {
                if let Some(last_failure_ts) = ctx.task_status.last_failure.map(|ts| ts.timestamp()) {
                    ELIGIBILITY_CTX_TASK_LAST_FAILURE.set(last_failure_ts);
                }

                ELIGIBILITY_CTX_ALL_SINKS_HEALTHY.set(ctx.all_sinks_healthy as i64);
                ELIGIBILITY_CTX_CLUSTER_IS_DEPLOYING.set(ctx.cluster_status.is_deploying as i64);
                ELIGIBILITY_CTX_CLUSTER_LAST_DEPLOYMENT.set(ctx.cluster_status.last_deployment.timestamp());
            }

            Err(err) => {
                tracing::warn!(
                    error=?err, %phase_name,
                    "failed to update eligibility context metrics on subscription: {}", subscription_name
                );
                proctor::track_errors(phase_name.as_ref(), &ProctorError::EligibilityError(err.into()));
            }
        };

        Box::new(update_fn)
    }
}

#[derive(PolarClass, Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(PolarClass, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterStatus {
    // todo: options to source property:
    // query k8s to describe job-manager pod in json and filter for .status.conditions
    // is_deploying is false until type:Ready has status:True
    // - poll Flink readiness probe; i.e., k8s pod status
    // - poll an ENVVAR or a cvs file; init-container can update accordingly
    #[polar(attribute)]
    #[serde(rename = "cluster.is_deploying")]
    pub is_deploying: bool,

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

pub(crate) static ELIGIBILITY_CTX_ALL_SINKS_HEALTHY: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "eligibility_ctx_all_sinks_healthy",
        "Are all sinks for the FLink jobs healthy",
    )
    .expect("failed creating eligibility_ctx_all_sinks_healthy")
});

pub(crate) static ELIGIBILITY_CTX_TASK_LAST_FAILURE: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "eligibility_ctx_task_last_failure",
        "UNIX timestamp in seconds of last Flink Task Manager failure in environment",
    )
    .expect("failed creating eligibility_ctx_task_last_failure metric")
});

pub(crate) static ELIGIBILITY_CTX_CLUSTER_IS_DEPLOYING: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "eligibility_ctx_cluster_is_deploying",
        "Is the Flink cluster actively deploying: 1=yes, 0=no",
    )
    .expect("failed creating eligibility_ctx_cluster_is_deploying metric")
});

pub(crate) static ELIGIBILITY_CTX_CLUSTER_LAST_DEPLOYMENT: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "eligibility_ctx_cluster_last_deployment",
        "UNIX timestamp in seconds of last deployment of the Flink cluster",
    )
    .expect("failed creating eligibility_ctx_cluster_last_deployment metric")
});

// /////////////////////////////////////////////////////
// // Unit Tests ///////////////////////////////////////

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use claim::*;
    use lazy_static::lazy_static;
    use proctor::elements::telemetry::ToTelemetry;
    use proctor::elements::{Telemetry, NANOS_KEY, SECS_KEY};
    use serde_test::{assert_tokens, Token};

    use super::*;

    lazy_static! {
        static ref DT_1: DateTime<Utc> = Utc::now();
        static ref DT_1_STR: String = format!("{}", DT_1.format("%+"));
        static ref DT_2: DateTime<Utc> = Utc::now() - chrono::Duration::hours(5);
        static ref DT_2_STR: String = format!("{}", DT_2.format("%+"));
    }

    #[test]
    #[ignore]
    fn test_serde_flink_eligibility_context() {
        let context = EligibilityContext {
            correlation_id: Id::direct("EligibilityContext", 0, "A"),
            timestamp: Timestamp::new(0, 0),
            all_sinks_healthy: true,
            task_status: TaskStatus { last_failure: Some(DT_1.clone()) },
            cluster_status: ClusterStatus { is_deploying: false, last_deployment: DT_2.clone() },
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
            Token::Str("task.last_failure"),
            Token::Some,
            Token::Map { len: Some(2) },
            Token::Str(SECS_KEY),
            Token::I64(DT_1.timestamp()),
            Token::Str(NANOS_KEY),
            Token::I64(DT_1.timestamp_subsec_nanos() as i64),
            Token::MapEnd,
            Token::Str("cluster.is_deploying"),
            Token::Bool(false),
            Token::Str("cluster.last_deployment"),
            Token::Map { len: Some(2) },
            Token::Str(SECS_KEY),
            Token::I64(DT_2.timestamp()),
            Token::Str(NANOS_KEY),
            Token::I64(DT_2.timestamp_subsec_nanos() as i64),
            Token::MapEnd,
            Token::Str("custom_foo"),
            Token::Str("fred flintstone"),
            Token::Str("custom_bar"),
            Token::Str("The Happy Barber"),
            Token::MapEnd,
        ];

        let mut result = std::panic::catch_unwind(|| {
            assert_tokens(&context, expected.as_slice());
        });

        if result.is_err() {
            expected.swap(4, 6);
            expected.swap(5, 7);
            result = std::panic::catch_unwind(|| {
                assert_tokens(&context, expected.as_slice());
            })
        }

        if result.is_err() {
            expected.swap(13, 15);
            expected.swap(14, 16);
            result = std::panic::catch_unwind(|| {
                assert_tokens(&context, expected.as_slice());
            })
        }

        if result.is_err() {
            expected.swap(4, 6);
            expected.swap(5, 7);
            result = std::panic::catch_unwind(|| {
                assert_tokens(&context, expected.as_slice());
            })
        }

        if result.is_err() {
            panic!("{:?}", result);
        }
    }

    #[test]
    fn test_serde_flink_eligibility_context_from_telemetry() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);

        let data: Telemetry = maplit::hashmap! {
            "correlation_id" => Id::<EligibilityContext>::direct("EligibilityContext", 0, "A").to_telemetry(),
            "timestamp" => Timestamp::new(0, 0).to_telemetry(),
            "all_sinks_healthy" => false.to_telemetry(),
            "task.last_failure" => DT_1_STR.as_str().to_telemetry(),
            "cluster.is_deploying" => false.to_telemetry(),
            "cluster.last_deployment" => DT_2_STR.as_str().to_telemetry(),
            "foo" => "bar".to_telemetry(),
        }
        .into_iter()
        .collect();

        tracing::info!(telemetry=?data, "created telemetry");

        let actual = assert_ok!(data.try_into::<EligibilityContext>());
        tracing::info!(?actual, "converted into FlinkEligibilityContext");
        let expected = EligibilityContext {
            correlation_id: Id::direct("EligibilityContext", 0, "A"),
            timestamp: Timestamp::new(0, 0),
            all_sinks_healthy: false,
            task_status: TaskStatus { last_failure: Some(DT_1.clone()) },
            cluster_status: ClusterStatus { is_deploying: false, last_deployment: DT_2.clone() },
            custom: maplit::hashmap! {"foo".to_string() => "bar".into(),},
        };
        tracing::info!("actual: {:?}", actual);
        tracing::info!("expected: {:?}", expected);
        assert_eq!(actual, expected);
    }
}
