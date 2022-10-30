use once_cell::sync::Lazy;

use crate::phases::eligibility::{ClusterStatus, EligibilityContext, JobStatus};

mod context {
    use chrono::{DateTime, Utc};
    use claim::*;
    use proctor::elements::telemetry::ToTelemetry;
    use proctor::elements::{Telemetry, NANOS_KEY, SECS_KEY};
    use serde_test::{assert_tokens, Token};

    use super::*;

    static DT_1: Lazy<DateTime<Utc>> = Lazy::new(|| Utc::now());
    static DT_1_STR: Lazy<String> = Lazy::new(|| format!("{}", DT_1.format("%+")));
    static DT_2: Lazy<DateTime<Utc>> = Lazy::new(|| Utc::now() - chrono::Duration::hours(5));
    static DT_2_STR: Lazy<String> = Lazy::new(|| format!("{}", DT_2.format("%+")));

    #[test]
    #[ignore = "asserting a map is difficult given random ordering for each run"]
    fn test_serde_flink_eligibility_context() {
        let context = EligibilityContext {
            all_sinks_healthy: true,
            job: JobStatus { last_failure: Some(DT_1.clone()) },
            cluster: ClusterStatus {
                is_deploying: false,
                is_rescaling: false,
                last_deployment: DT_2.clone(),
            },
            custom: maplit::hashmap! {
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
            Token::Str(NANOS_KEY),
            Token::I64(DT_1.timestamp_subsec_nanos() as i64),
            Token::MapEnd,
            Token::Str("cluster.is_deploying"),
            Token::Bool(false),
            Token::Str("cluster.is_rescaling"),
            Token::Bool(false),
            Token::Str("cluster.last_deployment"),
            Token::Map { len: Some(2) },
            Token::Str(SECS_KEY),
            Token::I64(DT_2.timestamp()),
            Token::Str(NANOS_KEY),
            Token::I64(DT_2.timestamp_subsec_nanos() as i64),
            Token::MapEnd,
            Token::Str("all_sinks_healthy"),
            Token::Bool(true),
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
            all_sinks_healthy: false,
            job: JobStatus { last_failure: Some(DT_1.clone()) },
            cluster: ClusterStatus {
                is_deploying: false,
                is_rescaling: false,
                last_deployment: DT_2.clone(),
            },
            custom: maplit::hashmap! {"foo".to_string() => "bar".into(),},
        };
        tracing::info!("actual: {:?}", actual);
        tracing::info!("expected: {:?}", expected);
        assert_eq!(actual, expected);
    }
}
