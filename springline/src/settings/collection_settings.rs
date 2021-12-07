use std::collections::HashMap;

use proctor::error::IncompatibleSourceSettingsError;
use proctor::phases::collection::SourceSetting;
use serde::{Deserialize, Serialize};

mod flink_settings;
mod metric_order;

pub use flink_settings::FlinkSettings;
pub use metric_order::{Aggregation, FlinkScope, MetricOrder};

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct CollectionSettings {
    pub flink: FlinkSettings,
    pub sources: HashMap<String, SourceSetting>,
}


#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use proctor::elements::telemetry::TelemetryType;
    use proctor::phases::collection::HttpQuery;
    use reqwest::header::{AUTHORIZATION, CONTENT_LENGTH};
    use reqwest::{Method, Url};
    use serde_test::{assert_tokens, Token};

    use super::*;

    #[test]
    fn test_serde_collection_settings() {
        let settings_csv = CollectionSettings {
            // only doing one pair at a time until *convenient* way to pin order and test is determined
            flink: FlinkSettings {
                job_manager_host: "dr-flink-jm-0".to_string(),
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
                ..FlinkSettings::default()
            },
            sources: maplit::hashmap! {
                "foo".to_string() => SourceSetting::Csv { path: PathBuf::from("./resources/bar.toml"),},
            },
        };

        assert_tokens(
            &settings_csv,
            &vec![
                Token::Struct { name: "CollectionSettings", len: 2 },
                Token::Str("flink"),
                Token::Struct { name: "FlinkSettings", len: 7 },
                Token::Str("job_manager_scheme"),
                Token::Str("https"),
                Token::Str("job_manager_host"),
                Token::Str("dr-flink-jm-0"),
                Token::Str("job_manager_port"),
                Token::U16(8081),
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
                Token::Str("max_retries"),
                Token::U32(3),
                Token::StructEnd,
                Token::Str("sources"),
                Token::Map { len: Some(1) },
                Token::Str("foo"),
                Token::Struct { name: "SourceSetting", len: 2 },
                Token::Str("type"),
                Token::Str("csv"),
                Token::Str("path"),
                Token::Str("./resources/bar.toml"),
                Token::StructEnd,
                Token::MapEnd,
                Token::StructEnd,
            ],
        );

        let settings_rest = CollectionSettings {
            flink: FlinkSettings {
                job_manager_scheme: "https".to_string(),
                job_manager_host: "dr-flink-jm-0".to_string(),
                job_manager_port: 8081,
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
                headers: vec![(reqwest::header::AUTHORIZATION.to_string(), "foobar".to_string())],
                max_retries: 5,
            },
            // only doing one pair at a time until *convenient* way to pin order and test is determined
            sources: maplit::hashmap! {
                "foo".to_string() => SourceSetting::RestApi(HttpQuery {
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
                Token::Struct { name: "CollectionSettings", len: 2 },
                Token::Str("flink"),
                Token::Struct { name: "FlinkSettings", len: 8 },
                Token::Str("job_manager_scheme"),
                Token::Str("https"),
                Token::Str("job_manager_host"),
                Token::Str("dr-flink-jm-0"),
                Token::Str("job_manager_port"),
                Token::U16(8081),
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
                Token::Str("headers"),
                Token::Seq { len: Some(1) },
                Token::Tuple { len: 2 },
                Token::Str("authorization"),
                Token::Str("foobar"),
                Token::TupleEnd,
                Token::SeqEnd,
                Token::Str("max_retries"),
                Token::U32(5),
                Token::StructEnd,
                Token::Str("sources"),
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
}
