use std::collections::HashMap;
use std::time::Duration;

use proctor::phases::sense::SensorSetting;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use crate::phases::sense::flink::MetricOrder;
pub use crate::settings::flink_settings::FlinkSettings;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default)]
#[cfg_attr(test, derive(PartialEq))]
pub struct SensorSettings {
    pub flink: FlinkSensorSettings,
    pub sensors: HashMap<String, SensorSetting>,
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[cfg_attr(test, derive(PartialEq))]
pub struct FlinkSensorSettings {
    #[serde(rename = "metrics_initial_delay_secs")]
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    pub metrics_initial_delay: Duration,

    #[serde(rename = "metrics_interval_secs")]
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    pub metrics_interval: Duration,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metric_orders: Vec<MetricOrder>,
}

impl Default for FlinkSensorSettings {
    fn default() -> Self {
        Self {
            metrics_initial_delay: Duration::from_secs(2 * 60),
            metrics_interval: Duration::from_secs(15),
            metric_orders: Vec::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use proctor::elements::telemetry::TelemetryType;
    use proctor::phases::sense::HttpQuery;
    use reqwest::header::{AUTHORIZATION, CONTENT_LENGTH};
    use reqwest::{Method, Url};
    use serde_test::{assert_tokens, Token};

    use super::*;
    use crate::phases::sense::flink::{Aggregation, FlinkScope, MetricOrder};
    use claim::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_serde_sensor_settings_1() {
        let settings_csv = SensorSettings {
            // only doing one pair at a time until *convenient* way to pin order and test is determined
            flink: FlinkSensorSettings {
                metric_orders: vec![
                    MetricOrder {
                        scope: FlinkScope::Jobs,
                        metric: "uptime".to_string(),
                        agg: Aggregation::Max,
                        telemetry_path: "health.job_uptime_millis".to_string(),
                        telemetry_type: TelemetryType::Integer,
                    },
                    MetricOrder {
                        scope: FlinkScope::Kafka,
                        metric: "records-lag-max".to_string(),
                        agg: Aggregation::Value,
                        telemetry_path: "flow.input_records_lag_max".to_string(),
                        telemetry_type: TelemetryType::Integer,
                    },
                    MetricOrder {
                        scope: FlinkScope::TaskManagers,
                        metric: "Status.JVM.Memory.Heap.Committed".to_string(),
                        agg: Aggregation::Sum,
                        telemetry_path: "cluster.task_heap_memory_committed".to_string(),
                        telemetry_type: TelemetryType::Float,
                    },
                ],
                ..FlinkSensorSettings::default()
            },
            sensors: maplit::hashmap! {
                "foo".to_string() => SensorSetting::Csv { path: PathBuf::from("./resources/bar.toml"),},
            },
        };

        assert_tokens(
            &settings_csv,
            &vec![
                Token::Struct { name: "SensorSettings", len: 2 },
                Token::Str("flink"),
                Token::Struct { name: "FlinkSensorSettings", len: 3 },
                Token::Str("metrics_initial_delay_secs"),
                Token::U64(120),
                Token::Str("metrics_interval_secs"),
                Token::U64(15),
                Token::Str("metric_orders"),
                Token::Seq { len: Some(3) },
                Token::TupleStruct { name: "MetricOrder", len: 5 },
                Token::UnitVariant { name: "FlinkScope", variant: "Jobs" },
                Token::Str("uptime"),
                Token::UnitVariant { name: "Aggregation", variant: "max" },
                Token::Str("health.job_uptime_millis"),
                Token::UnitVariant { name: "TelemetryType", variant: "Integer" },
                Token::TupleStructEnd,
                Token::TupleStruct { name: "MetricOrder", len: 5 },
                Token::UnitVariant { name: "FlinkScope", variant: "Kafka" },
                Token::Str("records-lag-max"),
                Token::UnitVariant { name: "Aggregation", variant: "value" },
                Token::Str("flow.input_records_lag_max"),
                Token::UnitVariant { name: "TelemetryType", variant: "Integer" },
                Token::TupleStructEnd,
                Token::TupleStruct { name: "MetricOrder", len: 5 },
                Token::UnitVariant { name: "FlinkScope", variant: "TaskManagers" },
                Token::Str("Status.JVM.Memory.Heap.Committed"),
                Token::UnitVariant { name: "Aggregation", variant: "sum" },
                Token::Str("cluster.task_heap_memory_committed"),
                Token::UnitVariant { name: "TelemetryType", variant: "Float" },
                Token::TupleStructEnd,
                Token::SeqEnd,
                Token::StructEnd,
                Token::Str("sensors"),
                Token::Map { len: Some(1) },
                Token::Str("foo"),
                Token::Struct { name: "SensorSetting", len: 2 },
                Token::Str("type"),
                Token::Str("csv"),
                Token::Str("path"),
                Token::Str("./resources/bar.toml"),
                Token::StructEnd,
                Token::MapEnd,
                Token::StructEnd,
            ],
        );
    }

    #[test]
    fn test_serde_sensor_settings_2() {
        let settings_rest = SensorSettings {
            flink: FlinkSensorSettings {
                metrics_initial_delay: Duration::from_secs(300),
                metrics_interval: Duration::from_secs(15),
                metric_orders: vec![
                    MetricOrder {
                        scope: FlinkScope::Task,
                        metric: "Status.JVM.Memory.NonHeap.Committed".to_string(),
                        agg: Aggregation::Max,
                        telemetry_path: "cluster.task_heap_memory_committed".to_string(),
                        telemetry_type: TelemetryType::Float,
                    },
                    MetricOrder {
                        scope: FlinkScope::Jobs,
                        metric: "uptime".to_string(),
                        agg: Aggregation::Min,
                        telemetry_path: "health.job_uptime_millis".to_string(),
                        telemetry_type: TelemetryType::Integer,
                    },
                ],
            },
            // only doing one pair at a time until *convenient* way to pin order and test is determined
            sensors: maplit::hashmap! {
                "foo".to_string() => SensorSetting::RestApi(HttpQuery {
                    interval: Duration::from_secs(7),
                    method: Method::GET,
                    url: Url::parse("https://www.example.com/foobar").unwrap(),
                    headers: vec![
                        (CONTENT_LENGTH.to_string(), 17_i64.to_string()),
                        (AUTHORIZATION.to_string(), "Bearer".to_string()),
                    ],
                    max_retries: 3,
                }),
            },
        };

        assert_tokens(
            &settings_rest,
            &vec![
                Token::Struct { name: "SensorSettings", len: 2 },
                Token::Str("flink"),
                Token::Struct { name: "FlinkSensorSettings", len: 3 },
                Token::Str("metrics_initial_delay_secs"),
                Token::U64(300),
                Token::Str("metrics_interval_secs"),
                Token::U64(15),
                Token::Str("metric_orders"),
                Token::Seq { len: Some(2) },
                Token::TupleStruct { name: "MetricOrder", len: 5 },
                Token::UnitVariant { name: "FlinkScope", variant: "Task" },
                Token::Str("Status.JVM.Memory.NonHeap.Committed"),
                Token::UnitVariant { name: "Aggregation", variant: "max" },
                Token::Str("cluster.task_heap_memory_committed"),
                Token::UnitVariant { name: "TelemetryType", variant: "Float" },
                Token::TupleStructEnd,
                Token::TupleStruct { name: "MetricOrder", len: 5 },
                Token::UnitVariant { name: "FlinkScope", variant: "Jobs" },
                Token::Str("uptime"),
                Token::UnitVariant { name: "Aggregation", variant: "min" },
                Token::Str("health.job_uptime_millis"),
                Token::UnitVariant { name: "TelemetryType", variant: "Integer" },
                Token::TupleStructEnd,
                Token::SeqEnd,
                Token::StructEnd,
                Token::Str("sensors"),
                Token::Map { len: Some(1) },
                Token::Str("foo"),
                Token::Struct { name: "HttpQuery", len: 6 },
                Token::Str("type"),
                Token::Str("rest_api"),
                Token::Str("interval_secs"),
                Token::U64(7),
                Token::Str("method"),
                Token::Str("GET"),
                Token::Str("url"),
                Token::Str("https://www.example.com/foobar"),
                Token::Str("headers"),
                Token::Seq { len: Some(2) },
                Token::Tuple { len: 2 },
                Token::Str(CONTENT_LENGTH.as_str()),
                Token::Str("17"),
                Token::TupleEnd,
                Token::Tuple { len: 2 },
                Token::Str(AUTHORIZATION.as_str()),
                Token::Str("Bearer"),
                Token::TupleEnd,
                Token::SeqEnd,
                Token::Str("max_retries"),
                Token::U32(3),
                Token::StructEnd,
                Token::MapEnd,
                Token::StructEnd,
            ],
        );
    }

    #[test]
    fn test_flink_metric_order_serde() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::debug_span!("test_flink_metric_order_serde");
        let _main_span_guard = main_span.enter();

        let metric_orders = vec![
            MetricOrder {
                scope: FlinkScope::Jobs,
                metric: "uptime".to_string(),
                agg: Aggregation::Max,
                telemetry_path: "health.job_uptime_millis".to_string(),
                telemetry_type: TelemetryType::Integer,
            },
            MetricOrder {
                scope: FlinkScope::Kafka,
                metric: "records-lag-max".to_string(),
                agg: Aggregation::Value,
                telemetry_path: "flow.input_records_lag_max".to_string(),
                telemetry_type: TelemetryType::Integer,
            },
            MetricOrder {
                scope: FlinkScope::TaskManagers,
                metric: "Status.JVM.Memory.Heap.Committed".to_string(),
                agg: Aggregation::Sum,
                telemetry_path: "cluster.task_heap_memory_committed".to_string(),
                telemetry_type: TelemetryType::Float,
            },
        ];

        let actual = assert_ok!(ron::to_string(&metric_orders));
        assert_eq!(
            actual,
            r##"[(Jobs,"uptime",max,"health.job_uptime_millis",Integer),(Kafka,"records-lag-max",value,"flow.input_records_lag_max",Integer),(TaskManagers,"Status.JVM.Memory.Heap.Committed",sum,"cluster.task_heap_memory_committed",Float)]"##
        );

        let hydrated: Vec<MetricOrder> = assert_ok!(ron::from_str(actual.as_str()));
        assert_eq!(hydrated, metric_orders);
    }
}
