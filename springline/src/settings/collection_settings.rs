use std::collections::HashMap;
use std::fmt::{self, Display};
use std::str::FromStr;
use std::time::Duration;

use itertools::Itertools;
use proctor::elements::TelemetryType;
use proctor::error::IncompatibleSourceSettingsError;
use proctor::phases::collection::SourceSetting;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::de::{self, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::ser::SerializeTupleStruct;
use serde::{Deserialize, Serialize, Serializer};
use serde_with::{serde_as, DurationSeconds};
use url::Url;

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct CollectionSettings {
    pub flink: FlinkSettings,
    pub sources: HashMap<String, SourceSetting>,
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct FlinkSettings {
    #[serde(default = "FlinkSettings::default_job_manager_scheme")]
    pub job_manager_scheme: String,

    #[serde(default = "FlinkSettings::default_job_manager_host")]
    pub job_manager_host: String,

    #[serde(default = "FlinkSettings::default_job_manager_port")]
    pub job_manager_port: u16,

    #[serde(rename = "metrics_initial_delay_secs")]
    #[serde_as(as = "DurationSeconds<u64>")]
    pub metrics_initial_delay: Duration,

    #[serde(rename = "metrics_interval_secs")]
    #[serde_as(as = "DurationSeconds<u64>")]
    pub metrics_interval: Duration,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metric_orders: Vec<FlinkMetricOrder>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub headers: Vec<(String, String)>,

    #[serde(default = "FlinkSettings::default_max_retries")]
    pub max_retries: u32,
}

impl Default for FlinkSettings {
    fn default() -> Self {
        Self {
            job_manager_scheme: Self::DEFAULT_JOB_MANAGER_SCHEME.to_string(),
            job_manager_host: Self::DEFAULT_JOB_MANAGER_HOST.to_string(),
            job_manager_port: Self::DEFAULT_JOB_MANAGER_PORT,
            metrics_initial_delay: Duration::from_secs(2 * 60),
            metrics_interval: Duration::from_secs(15),
            metric_orders: Vec::default(),
            headers: Vec::default(),
            max_retries: Self::DEFAULT_MAX_RETRIES,
        }
    }
}

impl FlinkSettings {
    const DEFAULT_JOB_MANAGER_HOST: &'static str = "localhost";
    const DEFAULT_JOB_MANAGER_PORT: u16 = 8081;
    const DEFAULT_JOB_MANAGER_SCHEME: &'static str = "https";
    const DEFAULT_MAX_RETRIES: u32 = 3;

    pub fn default_job_manager_scheme() -> String {
        Self::DEFAULT_JOB_MANAGER_SCHEME.to_string()
    }

    pub fn default_job_manager_host() -> String {
        Self::DEFAULT_JOB_MANAGER_HOST.to_string()
    }

    pub fn default_job_manager_port() -> u16 {
        Self::DEFAULT_JOB_MANAGER_PORT
    }

    pub fn default_max_retries() -> u32 {
        Self::DEFAULT_MAX_RETRIES
    }

    pub fn job_manager_url(&self, scheme: impl AsRef<str>) -> Result<Url, url::ParseError> {
        let rep = format!(
            "{}//{}:{}",
            scheme.as_ref(),
            self.job_manager_host,
            self.job_manager_port
        );
        Url::parse(rep.as_str())
    }

    pub fn header_map(&self) -> Result<HeaderMap, IncompatibleSourceSettingsError> {
        let mut result = HeaderMap::with_capacity(self.headers.len());

        for (k, v) in self.headers.iter() {
            let name = HeaderName::from_str(k.as_str())?;
            let value = HeaderValue::from_str(v.as_str())?;
            result.insert(name, value);
        }

        Ok(result)
    }

    pub fn base_url(&self) -> Result<Url, url::ParseError> {
        Url::parse(
            format!(
                "{}://{}:{}/",
                self.job_manager_scheme, self.job_manager_host, self.job_manager_port
            )
            .as_str(),
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FlinkMetricOrder {
    pub scope: FlinkScope,
    pub metric: String,
    pub agg: Aggregation,
    pub telemetry_path: String,
    pub telemetry_type: TelemetryType,
}

impl FlinkMetricOrder {
    pub fn organize_by_scope(orders: &[FlinkMetricOrder]) -> HashMap<FlinkScope, Vec<FlinkMetricOrder>> {
        let mut result: HashMap<FlinkScope, Vec<FlinkMetricOrder>> = HashMap::default();

        for (scope, group) in &orders.iter().group_by(|o| o.scope) {
            let orders = result.entry(scope).or_insert_with(Vec::new);
            let group: Vec<FlinkMetricOrder> = group.cloned().collect();
            orders.extend(group);
        }

        result
    }
}

impl Serialize for FlinkMetricOrder {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_tuple_struct("FlinkMetricOrder", 5)?;
        state.serialize_field(&self.scope)?;
        state.serialize_field(&self.metric)?;
        state.serialize_field(&self.agg)?;
        state.serialize_field(&self.telemetry_path)?;
        state.serialize_field(&self.telemetry_type)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for FlinkMetricOrder {
    #[tracing::instrument(level = "debug", skip(deserializer))]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "snake_case")]
        enum Field {
            Scope,
            Metric,
            Agg,
            TelemetryPath,
            TelemetryType,
        }

        struct OrderVisitor;

        impl<'de> Visitor<'de> for OrderVisitor {
            type Value = FlinkMetricOrder;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("struct FlinkMetricOrder")
            }

            #[tracing::instrument(level = "debug", skip(self, seq))]
            fn visit_seq<V>(self, mut seq: V) -> Result<Self::Value, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let scope = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let metric = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let agg = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(2, &self))?;
                let telemetry_path = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(3, &self))?;
                let telemetry_type = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(4, &self))?;

                Ok(FlinkMetricOrder { scope, metric, agg, telemetry_path, telemetry_type })
            }

            #[tracing::instrument(level = "debug", skip(self, map))]
            fn visit_map<V>(self, mut map: V) -> Result<Self::Value, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut scope = None;
                let mut metric = None;
                let mut agg = None;
                let mut telemetry_path = None;
                let mut telemetry_type = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Scope => {
                            if scope.is_some() {
                                return Err(de::Error::duplicate_field("scope"));
                            }
                            scope = Some(map.next_value()?);
                        },
                        Field::Metric => {
                            if metric.is_some() {
                                return Err(de::Error::duplicate_field("metric"));
                            }
                            metric = Some(map.next_value()?);
                        },
                        Field::Agg => {
                            if agg.is_some() {
                                return Err(de::Error::duplicate_field("agg"));
                            }
                            agg = Some(map.next_value()?);
                        },
                        Field::TelemetryPath => {
                            if telemetry_path.is_some() {
                                return Err(de::Error::duplicate_field("telemetry_path"));
                            }
                            telemetry_path = Some(map.next_value()?);
                        },
                        Field::TelemetryType => {
                            if telemetry_type.is_some() {
                                return Err(de::Error::duplicate_field("telemetry_type"));
                            }
                            telemetry_type = Some(map.next_value()?);
                        },
                    }
                }

                let scope = scope.ok_or_else(|| de::Error::missing_field("scope"))?;
                let metric = metric.ok_or_else(|| de::Error::missing_field("metric"))?;
                let agg = agg.ok_or_else(|| de::Error::missing_field("agg"))?;
                let telemetry_path = telemetry_path.ok_or_else(|| de::Error::missing_field("telemetry_path"))?;
                let telemetry_type = telemetry_type.ok_or_else(|| de::Error::missing_field("telemetry_type"))?;

                Ok(FlinkMetricOrder { scope, metric, agg, telemetry_path, telemetry_type })
            }
        }

        const FIELDS: &'static [&'static str] = &["scope", "metric", "agg", "telemetry_path", "telemetry_type"];
        deserializer.deserialize_tuple_struct("FlinkMetricOrder", 5, OrderVisitor)
    }
}

#[derive(Debug, Display, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FlinkScope {
    Jobs,
    Task,
    TaskManagers,
    Kafka,
    Kinesis,
}

#[derive(Debug, Display, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Aggregation {
    #[serde(rename = "value")]
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
    fn test_flink_settings_default() {
        let actual: FlinkSettings = assert_ok!(ron::from_str("()"));
        assert_eq!(actual, FlinkSettings::default());

        let actual: FlinkSettings = assert_ok!(ron::from_str("(job_manager_port:80)"));
        assert_eq!(
            actual,
            FlinkSettings { job_manager_port: 80, ..FlinkSettings::default() }
        );
    }

    #[test]
    fn test_flink_metric_order_serde() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::debug_span!("test_flink_metric_order_serde");
        let _main_span_guard = main_span.enter();

        let metric_orders = vec![
            FlinkMetricOrder {
                scope: FlinkScope::Jobs,
                metric: "uptime".to_string(),
                agg: Aggregation::Max,
                telemetry_path: "health.job_uptime_millis".to_string(),
                telemetry_type: TelemetryType::Integer,
            },
            FlinkMetricOrder {
                scope: FlinkScope::Kafka,
                metric: "records-lag-max".to_string(),
                agg: Aggregation::None,
                telemetry_path: "flow.input_records_lag_max".to_string(),
                telemetry_type: TelemetryType::Integer,
            },
            FlinkMetricOrder {
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
                    FlinkMetricOrder {
                        scope: FlinkScope::Jobs,
                        metric: "uptime".to_string(),
                        agg: Aggregation::Max,
                        telemetry_path: "health.job_uptime_millis".to_string(),
                        telemetry_type: TelemetryType::Integer,
                    },
                    FlinkMetricOrder {
                        scope: FlinkScope::Kafka,
                        metric: "records-lag-max".to_string(),
                        agg: Aggregation::None,
                        telemetry_path: "flow.input_records_lag_max".to_string(),
                        telemetry_type: TelemetryType::Integer,
                    },
                    FlinkMetricOrder {
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
                Token::TupleStruct { name: "FlinkMetricOrder", len: 5 },
                Token::UnitVariant { name: "FlinkScope", variant: "Jobs" },
                Token::Str("uptime"),
                Token::UnitVariant { name: "Aggregation", variant: "max" },
                Token::Str("health.job_uptime_millis"),
                Token::UnitVariant { name: "TelemetryType", variant: "Integer" },
                Token::TupleStructEnd,
                Token::TupleStruct { name: "FlinkMetricOrder", len: 5 },
                Token::UnitVariant { name: "FlinkScope", variant: "Kafka" },
                Token::Str("records-lag-max"),
                Token::UnitVariant { name: "Aggregation", variant: "value" },
                Token::Str("flow.input_records_lag_max"),
                Token::UnitVariant { name: "TelemetryType", variant: "Integer" },
                Token::TupleStructEnd,
                Token::TupleStruct { name: "FlinkMetricOrder", len: 5 },
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
                    FlinkMetricOrder {
                        scope: FlinkScope::Task,
                        metric: "Status.JVM.Memory.NonHeap.Committed".to_string(),
                        agg: Aggregation::Max,
                        telemetry_path: "cluster.task_heap_memory_committed".to_string(),
                        telemetry_type: TelemetryType::Float,
                    },
                    FlinkMetricOrder {
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
                Token::TupleStruct { name: "FlinkMetricOrder", len: 5 },
                Token::UnitVariant { name: "FlinkScope", variant: "Task" },
                Token::Str("Status.JVM.Memory.NonHeap.Committed"),
                Token::UnitVariant { name: "Aggregation", variant: "max" },
                Token::Str("cluster.task_heap_memory_committed"),
                Token::UnitVariant { name: "TelemetryType", variant: "Float" },
                Token::TupleStructEnd,
                Token::TupleStruct { name: "FlinkMetricOrder", len: 5 },
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
