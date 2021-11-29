use std::collections::HashMap;

use proctor::phases::collection::SourceSetting;
use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct CollectionSettings {
    pub flink: FlinkSettings,
    pub sources: HashMap<String, SourceSetting>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct FlinkSettings {
    pub job_manager_host: String,
    pub job_manager_port: u16,
    pub metric_orders: Vec<FlinkMetricOrder>,
}

impl Default for FlinkSettings {
    fn default() -> Self {
        Self {
            job_manager_host: "localhost".to_string(),
            job_manager_port: 8081,
            metric_orders: Vec::default(),
        }
    }
}

impl FlinkSettings {
    pub fn job_manager_url(&self, scheme: impl AsRef<str>) -> Result<Url, url::ParseError> {
        let rep = format!(
            "{}//{}:{}",
            scheme.as_ref(),
            self.job_manager_host,
            self.job_manager_port
        );
        Url::parse(rep.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FlinkMetricOrder(pub FlinkScope, pub String, pub FlinkMetricAggregatedValue);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FlinkScope {
    Jobs,
    Task,
    TaskManagers,
    Kafka,
    Kinesis,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FlinkMetricAggregatedValue {
    None,
    Max,
    Min,
    Sum,
    Avg,
}


#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use claim::*;
    use pretty_assertions::assert_eq;
    use proctor::phases::collection::HttpQuery;
    use reqwest::header::{AUTHORIZATION, CONTENT_LENGTH};
    use reqwest::{Method, Url};
    use serde_test::{assert_tokens, Token};

    use super::*;

    #[test]
    fn test_flink_metric_order_serde() {
        let metric_orders = vec![
            FlinkMetricOrder(FlinkScope::Jobs, "uptime".to_string(), FlinkMetricAggregatedValue::Max),
            FlinkMetricOrder(
                FlinkScope::Kafka,
                "records-lag-max".to_string(),
                FlinkMetricAggregatedValue::None,
            ),
            FlinkMetricOrder(
                FlinkScope::TaskManagers,
                "Status.JVM.Memory.Heap.Committed".to_string(),
                FlinkMetricAggregatedValue::Sum,
            ),
        ];

        let actual = assert_ok!(ron::to_string(&metric_orders));
        assert_eq!(
            actual,
            r##"[(Jobs,"uptime",max),(Kafka,"records-lag-max",none),(TaskManagers,"Status.JVM.Memory.Heap.Committed",sum)]"##
        );

        let hydrated: Vec<FlinkMetricOrder> = assert_ok!(ron::from_str(actual.as_str()));
        assert_eq!(hydrated, metric_orders);
    }

    #[test]
    fn test_serde_collection_settings() {
        let settings_csv = CollectionSettings {
            // only doing one pair at a time until *convenient* way to pin order and test is determined
            flink: FlinkSettings {
                job_manager_host: "dr-flink-jm-0".to_string(),
                metric_orders: vec![
                    FlinkMetricOrder(FlinkScope::Jobs, "uptime".to_string(), FlinkMetricAggregatedValue::Max),
                    FlinkMetricOrder(
                        FlinkScope::Kafka,
                        "records-lag-max".to_string(),
                        FlinkMetricAggregatedValue::None,
                    ),
                    FlinkMetricOrder(
                        FlinkScope::TaskManagers,
                        "Status.JVM.Memory.Heap.Committed".to_string(),
                        FlinkMetricAggregatedValue::Sum,
                    ),
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
                Token::Struct { name: "FlinkSettings", len: 3 },
                Token::Str("job_manager_host"),
                Token::Str("dr-flink-jm-0"),
                Token::Str("job_manager_port"),
                Token::U16(8081),
                Token::Str("metric_orders"),
                Token::Seq { len: Some(3) },
                Token::TupleStruct { name: "FlinkMetricOrder", len: 3 },
                Token::UnitVariant { name: "FlinkScope", variant: "Jobs" },
                Token::Str("uptime"),
                Token::UnitVariant { name: "FlinkMetricAggregatedValue", variant: "max" },
                Token::TupleStructEnd,
                Token::TupleStruct { name: "FlinkMetricOrder", len: 3 },
                Token::UnitVariant { name: "FlinkScope", variant: "Kafka" },
                Token::Str("records-lag-max"),
                Token::UnitVariant {
                    name: "FlinkMetricAggregatedValue",
                    variant: "none",
                },
                Token::TupleStructEnd,
                Token::TupleStruct { name: "FlinkMetricOrder", len: 3 },
                Token::UnitVariant { name: "FlinkScope", variant: "TaskManagers" },
                Token::Str("Status.JVM.Memory.Heap.Committed"),
                Token::UnitVariant { name: "FlinkMetricAggregatedValue", variant: "sum" },
                Token::TupleStructEnd,
                Token::SeqEnd,
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
                job_manager_host: "dr-flink-jm-0".to_string(),
                job_manager_port: 8081,
                metric_orders: vec![
                    FlinkMetricOrder(
                        FlinkScope::Task,
                        "Status.JVM.Memory.NonHeap.Committed".to_string(),
                        FlinkMetricAggregatedValue::Max,
                    ),
                    FlinkMetricOrder(FlinkScope::Jobs, "uptime".to_string(), FlinkMetricAggregatedValue::Min),
                ],
            },
            // only doing one pair at a time until *convenient* way to pin order and test is determined
            sources: maplit::hashmap! {
                "foo".to_string() => SourceSetting::RestApi(HttpQuery {
                    interval: Duration::from_secs(7),
                    method: Method::GET,
                    url: Url::parse("https://www.example.com/foobar").unwrap(),
                    headers: vec![
                        (CONTENT_LENGTH.to_string(), 17.to_string()),
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
                Token::Struct { name: "FlinkSettings", len: 3 },
                Token::Str("job_manager_host"),
                Token::Str("dr-flink-jm-0"),
                Token::Str("job_manager_port"),
                Token::U16(8081),
                Token::Str("metric_orders"),
                Token::Seq { len: Some(2) },
                Token::TupleStruct { name: "FlinkMetricOrder", len: 3 },
                Token::UnitVariant { name: "FlinkScope", variant: "Task" },
                Token::Str("Status.JVM.Memory.NonHeap.Committed"),
                Token::UnitVariant { name: "FlinkMetricAggregatedValue", variant: "max" },
                Token::TupleStructEnd,
                Token::TupleStruct { name: "FlinkMetricOrder", len: 3 },
                Token::UnitVariant { name: "FlinkScope", variant: "Jobs" },
                Token::Str("uptime"),
                Token::UnitVariant { name: "FlinkMetricAggregatedValue", variant: "min" },
                Token::TupleStructEnd,
                Token::SeqEnd,
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
