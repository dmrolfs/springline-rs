use std::collections::HashSet;
use std::fmt::Debug;

use chrono::{DateTime, Utc};
use oso::PolarClass;
use serde::{Deserialize, Serialize};

use proctor::elements::telemetry;
use proctor::error::EligibilityError;
use proctor::phases::collection::SubscriptionRequirements;
use proctor::ProctorContext;

#[derive(PolarClass, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FlinkEligibilityContext {
    #[polar(attribute)]
    #[serde(flatten)] // current subscription mechanism only supports flatten keys
    pub task_status: TaskStatus,

    #[polar(attribute)]
    #[serde(flatten)] // current subscription mechanism only supports flatten keys
    pub cluster_status: ClusterStatus,

    #[polar(attribute)]
    #[serde(flatten)] // flatten to collect extra properties.
    pub custom: telemetry::Table,
}

impl SubscriptionRequirements for FlinkEligibilityContext {
    fn required_fields() -> HashSet<&'static str> {
        maplit::hashset! {
            "task.last_failure",
            "cluster.is_deploying",
            "cluster.last_deployment",
        }
    }
}

impl ProctorContext for FlinkEligibilityContext {
    type Error = EligibilityError;

    fn custom(&self) -> telemetry::Table {
        self.custom.clone()
    }
}

#[derive(PolarClass, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskStatus {
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
    #[polar(attribute)]
    #[serde(rename = "cluster.is_deploying")]
    pub is_deploying: bool,
    #[serde(with = "proctor::serde", rename = "cluster.last_deployment")]
    pub last_deployment: DateTime<Utc>,
}

impl ClusterStatus {
    pub fn last_deployment_within_seconds(&self, seconds: i64) -> bool {
        let boundary = Utc::now() - chrono::Duration::seconds(seconds);
        boundary < self.last_deployment
    }
}

// /////////////////////////////////////////////////////
// // Unit Tests ///////////////////////////////////////

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use lazy_static::lazy_static;
    use serde_test::{assert_tokens, Token};

    use super::*;
    use proctor::elements::telemetry::ToTelemetry;
    use proctor::elements::{Telemetry, NSECS_KEY, SECS_KEY};

    lazy_static! {
        static ref DT_1: DateTime<Utc> = Utc::now();
        static ref DT_1_STR: String = format!("{}", DT_1.format("%+"));
        static ref DT_2: DateTime<Utc> = Utc::now() - chrono::Duration::hours(5);
        static ref DT_2_STR: String = format!("{}", DT_2.format("%+"));
    }

    #[test]
    #[ignore]
    fn test_serde_flink_eligibility_context() {
        let context = FlinkEligibilityContext {
            task_status: TaskStatus {
                last_failure: Some(DT_1.clone()),
            },
            cluster_status: ClusterStatus {
                is_deploying: false,
                last_deployment: DT_2.clone(),
            },
            custom: maplit::hashmap! {
                "custom_foo".to_string() => "fred flintstone".into(),
                "custom_bar".to_string() => "The Happy Barber".into(),
            },
        };

        let mut expected = vec![
            Token::Map { len: None },
            Token::Str("task.last_failure"),
            Token::Some,
            Token::Map { len: Some(2) },
            Token::Str(SECS_KEY),
            Token::I64(DT_1.timestamp()),
            Token::Str(NSECS_KEY),
            Token::I64(DT_1.timestamp_subsec_nanos() as i64),
            Token::MapEnd,
            Token::Str("cluster.is_deploying"),
            Token::Bool(false),
            Token::Str("cluster.last_deployment"),
            Token::Map { len: Some(2) },
            Token::Str(SECS_KEY),
            Token::I64(DT_2.timestamp()),
            Token::Str(NSECS_KEY),
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
        lazy_static::initialize(&proctor::tracing::TEST_TRACING);

        let data: Telemetry = maplit::hashmap! {
            "task.last_failure" => DT_1_STR.as_str().to_telemetry(),
            "cluster.is_deploying" => false.to_telemetry(),
            "cluster.last_deployment" => DT_2_STR.as_str().to_telemetry(),
            "foo" => "bar".to_telemetry(),
        }
        .into_iter()
        .collect();

        tracing::info!(telemetry=?data, "created telemetry");

        let actual = data.try_into::<FlinkEligibilityContext>();
        tracing::info!(?actual, "converted into FlinkEligibilityContext");
        let expected = FlinkEligibilityContext {
            task_status: TaskStatus {
                last_failure: Some(DT_1.clone()),
            },
            cluster_status: ClusterStatus {
                is_deploying: false,
                last_deployment: DT_2.clone(),
            },
            custom: maplit::hashmap! {"foo".to_string() => "bar".into(),},
        };
        tracing::info!("actual: {:?}", actual);
        tracing::info!("expected: {:?}", expected);
        assert_eq!(actual.unwrap(), expected);
    }
}
