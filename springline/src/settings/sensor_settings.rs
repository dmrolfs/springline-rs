use std::collections::HashMap;
use std::time::Duration;

use proctor::phases::sense::clearinghouse::TelemetryCacheSettings;
use proctor::phases::sense::SensorSetting;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use crate::phases::sense::flink::MetricOrder;
pub use crate::settings::flink_settings::FlinkSettings;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default)]
#[cfg_attr(test, derive(PartialEq))]
pub struct SensorSettings {
    pub clearinghouse: TelemetryCacheSettings,
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

    use claim::*;
    use pretty_assertions::assert_eq;
    use proctor::elements::telemetry::TelemetryType;
    use proctor::phases::sense::clearinghouse::CacheTtl;
    use proctor::phases::sense::HttpQuery;
    use reqwest::header::{AUTHORIZATION, CONTENT_LENGTH};
    use reqwest::{Method, Url};
    use serde_test::{assert_tokens, Token};

    use super::*;
    use crate::phases::sense::flink::{Aggregation, MetricOrder, MetricSpec, PlanPositionSpec};
    use trim_margin::MarginTrimmable;
    use Aggregation::{Max, Min, Sum, Value};
    use TelemetryType::{Float, Integer};

    #[test]
    fn test_serde_sensor_settings_1() {
        let settings_csv = SensorSettings {
            clearinghouse: TelemetryCacheSettings::default(),
            // only doing one pair at a time until *convenient* way to pin order and test is determined
            flink: FlinkSensorSettings {
                metric_orders: vec![
                    MetricOrder::Job {
                        metric: MetricSpec::new("uptime", Max, "health.job_uptime_millis", Integer),
                    },
                    MetricOrder::Operator {
                        name: "Source: Baz input".to_string(),
                        position: PlanPositionSpec::Source,
                        metric: MetricSpec::new("records-lag-max", Value, "flow.source_records_lag_max", Integer),
                    },
                    MetricOrder::TaskManager {
                        metric: MetricSpec::new(
                            "Status.JVM.Memory.Heap.Committed",
                            Sum,
                            "cluster.task_heap_memory_committed",
                            Float,
                        ),
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
                Token::Struct { name: "SensorSettings", len: 3 },
                Token::Str("clearinghouse"),
                Token::Struct { name: "TelemetryCacheSettings", len: 5 },
                Token::Str("ttl"),
                Token::Struct { name: "CacheTtl", len: 1 },
                Token::Str("default_ttl_secs"),
                Token::U64(300),
                Token::StructEnd,
                Token::Str("nr_counters"),
                Token::U64(1_000),
                Token::Str("max_cost"),
                Token::I64(100),
                Token::Str("incremental_item_cost"),
                Token::I64(1),
                Token::Str("cleanup_interval_millis"),
                Token::U64(5000),
                Token::StructEnd,
                Token::Str("flink"),
                Token::Struct { name: "FlinkSensorSettings", len: 3 },
                Token::Str("metrics_initial_delay_secs"),
                Token::U64(120),
                Token::Str("metrics_interval_secs"),
                Token::U64(15),
                Token::Str("metric_orders"),
                Token::Seq { len: Some(3) },
                Token::NewtypeVariant { name: "MetricOrder", variant: "Job" },
                Token::Map { len: None },
                Token::Str("metric"),
                Token::Str("uptime"),
                Token::Str("agg"),
                Token::UnitVariant { name: "Aggregation", variant: "max" },
                Token::Str("telemetry_path"),
                Token::Str("health.job_uptime_millis"),
                Token::Str("telemetry_type"),
                Token::UnitVariant { name: "TelemetryType", variant: "Integer" },
                Token::MapEnd,
                Token::NewtypeVariant { name: "MetricOrder", variant: "Operator" },
                Token::Map { len: None },
                Token::Str("name"),
                Token::Str("Source: Baz input"),
                Token::Str("position"),
                Token::UnitVariant { name: "PlanPositionSpec", variant: "source" },
                Token::Str("metric"),
                Token::Str("records-lag-max"),
                Token::Str("agg"),
                Token::UnitVariant { name: "Aggregation", variant: "value" },
                Token::Str("telemetry_path"),
                Token::Str("flow.source_records_lag_max"),
                Token::Str("telemetry_type"),
                Token::UnitVariant { name: "TelemetryType", variant: "Integer" },
                Token::MapEnd,
                Token::NewtypeVariant { name: "MetricOrder", variant: "TaskManager" },
                Token::Map { len: None },
                Token::Str("metric"),
                Token::Str("Status.JVM.Memory.Heap.Committed"),
                Token::Str("agg"),
                Token::UnitVariant { name: "Aggregation", variant: "sum" },
                Token::Str("telemetry_path"),
                Token::Str("cluster.task_heap_memory_committed"),
                Token::Str("telemetry_type"),
                Token::UnitVariant { name: "TelemetryType", variant: "Float" },
                Token::MapEnd,
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
            clearinghouse: TelemetryCacheSettings {
                ttl: CacheTtl {
                    never_expire: maplit::hashset! { "foo".to_string(), },
                    ..CacheTtl::default()
                },
                ..TelemetryCacheSettings::default()
            },
            flink: FlinkSensorSettings {
                metrics_initial_delay: Duration::from_secs(300),
                metrics_interval: Duration::from_secs(15),
                metric_orders: vec![
                    MetricOrder::Task {
                        position: PlanPositionSpec::Any,
                        metric: MetricSpec::new(
                            "Status.JVM.Memory.NonHeap.Committed",
                            Max,
                            "cluster.task_heap_memory_committed",
                            Float,
                        ),
                    },
                    MetricOrder::Job {
                        metric: MetricSpec::new("uptime", Min, "health.job_uptime_millis", Integer),
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
                Token::Struct { name: "SensorSettings", len: 3 },
                Token::Str("clearinghouse"),
                Token::Struct { name: "TelemetryCacheSettings", len: 5 },
                Token::Str("ttl"),
                Token::Struct { name: "CacheTtl", len: 2 },
                Token::Str("default_ttl_secs"),
                Token::U64(300),
                Token::Str("never_expire"),
                Token::Seq { len: Some(1) },
                Token::Str("foo"),
                Token::SeqEnd,
                Token::StructEnd,
                Token::Str("nr_counters"),
                Token::U64(1_000),
                Token::Str("max_cost"),
                Token::I64(100),
                Token::Str("incremental_item_cost"),
                Token::I64(1),
                Token::Str("cleanup_interval_millis"),
                Token::U64(5000),
                Token::StructEnd,
                Token::Str("flink"),
                Token::Struct { name: "FlinkSensorSettings", len: 3 },
                Token::Str("metrics_initial_delay_secs"),
                Token::U64(300),
                Token::Str("metrics_interval_secs"),
                Token::U64(15),
                Token::Str("metric_orders"),
                Token::Seq { len: Some(2) },
                Token::NewtypeVariant { name: "MetricOrder", variant: "Task" },
                Token::Map { len: None },
                Token::Str("metric"),
                Token::Str("Status.JVM.Memory.NonHeap.Committed"),
                Token::Str("agg"),
                Token::UnitVariant { name: "Aggregation", variant: "max" },
                Token::Str("telemetry_path"),
                Token::Str("cluster.task_heap_memory_committed"),
                Token::Str("telemetry_type"),
                Token::UnitVariant { name: "TelemetryType", variant: "Float" },
                Token::MapEnd,
                Token::NewtypeVariant { name: "MetricOrder", variant: "Job" },
                Token::Map { len: None },
                Token::Str("metric"),
                Token::Str("uptime"),
                Token::Str("agg"),
                Token::UnitVariant { name: "Aggregation", variant: "min" },
                Token::Str("telemetry_path"),
                Token::Str("health.job_uptime_millis"),
                Token::Str("telemetry_type"),
                Token::UnitVariant { name: "TelemetryType", variant: "Integer" },
                Token::MapEnd,
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

        let metric_orders: Vec<MetricOrder> = vec![
            MetricOrder::Job {
                metric: MetricSpec::new("uptime", Max, "health.job_uptime_millis", Integer),
            },
            MetricOrder::Operator {
                name: "Input: The best data".to_string(),
                position: PlanPositionSpec::Source,
                metric: MetricSpec::new("records-lag-max", Value, "flow.source_records_lag_max", Integer),
            },
            MetricOrder::TaskManager {
                metric: MetricSpec::new(
                    "Status.JVM.Memory.Heap.Committed",
                    Sum,
                    "cluster.task_heap_memory_committed",
                    Float,
                ),
            },
        ];

        let actual = assert_ok!(serde_yaml::to_string(&metric_orders));
        assert_eq!(
            actual,
            r##"|---
                |- Job:
                |    metric: uptime
                |    agg: max
                |    telemetry_path: health.job_uptime_millis
                |    telemetry_type: Integer
                |- Operator:
                |    name: "Input: The best data"
                |    position: source
                |    metric: records-lag-max
                |    agg: value
                |    telemetry_path: flow.source_records_lag_max
                |    telemetry_type: Integer
                |- TaskManager:
                |    metric: Status.JVM.Memory.Heap.Committed
                |    agg: sum
                |    telemetry_path: cluster.task_heap_memory_committed
                |    telemetry_type: Float
                |"##
            .trim_margin_with("|")
            .unwrap()
        );

        let hydrated: Vec<MetricOrder> = assert_ok!(serde_yaml::from_str(actual.as_str()));
        assert_eq!(hydrated, metric_orders);
    }
}

// -- ron format --
// r##"|[
//                 |    Job({
//                 |        "metric": "uptime",
//                 |        "agg": max,
//                 |        "telemetry_path": "health.job_uptime_millis",
//                 |        "telemetry_type": Integer,
//                 |    }),
//                 |    Operator({
//                 |        "name": "Input: The best data",
//                 |        "metric": "records-lag-max",
//                 |        "agg": value,
//                 |        "telemetry_path": "flow.source_records_lag_max",
//                 |        "telemetry_type": Integer,
//                 |    }),
//                 |    TaskManager({
//                 |        "metric": "Status.JVM.Memory.Heap.Committed",
//                 |        "agg": sum,
//                 |        "telemetry_path": "cluster.task_heap_memory_committed",
//                 |        "telemetry_type": Float,
//                 |    }),
//                 |]"##
