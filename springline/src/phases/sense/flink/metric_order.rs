use anyhow::anyhow;
use std::collections::HashMap;
use std::fmt::{self, Display};

use itertools::Itertools;
use once_cell::sync::Lazy;
use proctor::elements::telemetry::combine::{self, TelemetryCombinator};
use proctor::elements::TelemetryType;
use proctor::error::SenseError;
use regex::Regex;
use serde::de::{self, Deserializer, MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize, Serializer};

use crate::phases::sense::flink::STD_METRIC_ORDERS;
use crate::settings::FlinkSensorSettings;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricSpec {
    pub metric: String,
    pub agg: Aggregation,
    pub telemetry_path: String,
    pub telemetry_type: TelemetryType,
}

impl MetricSpec {
    pub fn new(
        metric: impl Into<String>, agg: Aggregation, telemetry_path: impl Into<String>, telemetry_type: TelemetryType,
    ) -> Self {
        Self {
            metric: metric.into(),
            agg,
            telemetry_path: telemetry_path.into(),
            telemetry_type,
        }
    }
}

// impl PartialEq for MetricSpec {
//     fn eq(&self, other: &Self) -> bool {
//         self.agg == other.agg
//         && self.telemetry_type == other.telemetry_type
//         && self.telemetry_path == other.telemetry_path
//         && self.metric.as_str() == other.metric.as_str()
//         && self.telemetry_path == other.telemetry_path
//     }
// }
//
// impl Eq for MetricSpec { }

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeSpec {
    pub scope: FlinkScope,
    pub name: Option<String>,
}

//todo - do I need a way of saying literal or regex/partial?
//todo - refactor into enum by flinkscope since there will be ops driven on scope.
// #[derive(Debug, Clone, PartialEq, Eq)]
// pub struct MetricOrder {
//     pub scope: FlinkScope,
//     pub metric: String,
//     pub agg: Aggregation,
//     pub telemetry_path: String,
//     pub telemetry_type: TelemetryType,
// }
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetricOrder {
    Job(MetricSpec),
    TaskManager(MetricSpec),
    Task(MetricSpec),
    Operator(String, MetricSpec),
}

impl MetricOrder {
    pub fn from_scope_spec(scope: ScopeSpec, metric: MetricSpec) -> Result<Self, SenseError> {
        match scope.scope {
            FlinkScope::Job => Ok(Self::Job(metric)),
            FlinkScope::TaskManager => Ok(Self::TaskManager(metric)),
            FlinkScope::Task => Ok(Self::Task(metric)),
            FlinkScope::Operator if scope.name.is_some() => Ok(Self::Operator(scope.name.unwrap(), metric)),
            _ => Err(SenseError::Stage(anyhow!(
                "bad metric order scope specification: {:?}",
                scope
            ))),
        }
    }

    pub const fn scope(&self) -> FlinkScope {
        match self {
            Self::Job(_) => FlinkScope::Job,
            Self::TaskManager(_) => FlinkScope::TaskManager,
            Self::Task(_) => FlinkScope::Task,
            Self::Operator(_, _) => FlinkScope::Operator,
        }
    }

    pub fn scope_name(&self) -> Option<&str> {
        match self {
            Self::Operator(name, _) => Some(name.as_str()),
            _ => None,
        }
    }

    pub const fn agg(&self) -> Aggregation {
        self.metric_spec().agg
    }

    pub fn metric(&self) -> &str {
        self.metric_spec().metric.as_str()
    }

    pub fn telemetry(&self) -> (TelemetryType, &str) {
        let spec = self.metric_spec();
        (spec.telemetry_type, spec.telemetry_path.as_str())
    }

    const fn metric_spec(&self) -> &MetricSpec {
        match self {
            Self::Job(spec) => spec,
            Self::TaskManager(spec) => spec,
            Self::Task(spec) => spec,
            Self::Operator(_, spec) => spec,
        }
    }

    pub fn extend_standard_with_settings(settings: &FlinkSensorSettings) -> Vec<Self> {
        let mut orders = STD_METRIC_ORDERS.clone();
        orders.extend(settings.metric_orders.clone());
        orders
    }

    pub fn organize_by_scope(orders: &[Self]) -> HashMap<FlinkScope, Vec<Self>> {
        let mut result: HashMap<FlinkScope, Vec<Self>> = HashMap::default();

        for (scope, group) in &orders.iter().group_by(|o| o.scope()) {
            let orders = result.entry(scope).or_insert_with(Vec::new);
            let group: Vec<Self> = group.cloned().collect();
            orders.extend(group);
        }

        result
    }
}

impl Serialize for MetricOrder {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let (scope, scope_name, metric_spec, size) = match self {
            Self::Operator(s, m) => (FlinkScope::Operator, Some(s.as_str()), m, 6),
            Self::Job(m) => (FlinkScope::Job, None, m, 5),
            Self::TaskManager(m) => (FlinkScope::TaskManager, None, m, 5),
            Self::Task(m) => (FlinkScope::Task, None, m, 5),
        };

        let mut state = serializer.serialize_map(Some(size))?;
        state.serialize_entry("scope", &scope)?;
        if let Some(name) = scope_name {
            state.serialize_entry("name", name)?;
        }
        state.serialize_entry("metric", &metric_spec.metric)?;
        state.serialize_entry("agg", &metric_spec.agg)?;
        state.serialize_entry("telemetry_path", &metric_spec.telemetry_path)?;
        state.serialize_entry("telemetry_type", &metric_spec.telemetry_type)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for MetricOrder {
    #[tracing::instrument(level = "trace", skip(deserializer))]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "snake_case")]
        enum Field {
            Scope,
            #[serde(alias = "name")]
            ScopeName,
            Metric,
            Agg,
            TelemetryPath,
            TelemetryType,
        }

        struct OrderVisitor;

        impl<'de> Visitor<'de> for OrderVisitor {
            type Value = MetricOrder;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("map MetricOrder")
            }

            #[tracing::instrument(level = "trace", skip(self, map))]
            fn visit_map<V>(self, mut map: V) -> Result<Self::Value, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut scope = None;
                let mut scope_name = None;
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
                        Field::ScopeName => {
                            if scope_name.is_some() {
                                return Err(de::Error::duplicate_field("name"));
                            }
                            scope_name = Some(map.next_value()?);
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

                let scope_spec = ScopeSpec { scope, name: scope_name };
                let metric_spec = MetricSpec { metric, agg, telemetry_path, telemetry_type };
                MetricOrder::from_scope_spec(scope_spec, metric_spec).map_err(de::Error::custom)
            }
        }

        // const FIELDS: &'static [&'static str] = &["scope", "metric", "agg", "telemetry_path",
        // "telemetry_type"];

        deserializer.deserialize_map(OrderVisitor)
    }
}

#[derive(Debug, Display, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FlinkScope {
    Job,
    TaskManager,
    Task,
    Operator,
    // Kafka,
    // Kinesis,
    // Other,
}

static FLINK_SCOPE_JOB: Lazy<String> = Lazy::new(|| FlinkScope::Job.to_string());
static FLINK_SCOPE_TASKMANAGER: Lazy<String> = Lazy::new(|| FlinkScope::TaskManager.to_string());
static FLINK_SCOPE_TASK: Lazy<String> = Lazy::new(|| FlinkScope::Task.to_string());
static FLINK_SCOPE_OPERATOR: Lazy<String> = Lazy::new(|| FlinkScope::Operator.to_string());
// static FLINK_SCOPE_KAFKA: Lazy<String> = Lazy::new(|| FlinkScope::Kafka.to_string());
// static FLINK_SCOPE_KINESIS: Lazy<String> = Lazy::new(|| FlinkScope::Kinesis.to_string());
// static FLINK_SCOPE_OTHER: Lazy<String> = Lazy::new(|| FlinkScope::Other.to_string());

impl AsRef<str> for FlinkScope {
    fn as_ref(&self) -> &str {
        match self {
            Self::Job => FLINK_SCOPE_JOB.as_ref(),
            Self::Task => FLINK_SCOPE_TASK.as_ref(),
            Self::TaskManager => FLINK_SCOPE_TASKMANAGER.as_ref(),
            Self::Operator => FLINK_SCOPE_OPERATOR.as_ref(),
            // Self::Kafka => FLINK_SCOPE_KAFKA.as_ref(),
            // Self::Kinesis => FLINK_SCOPE_KINESIS.as_ref(),
            // Self::Other => FLINK_SCOPE_OTHER.as_ref(),
        }
    }
}

#[derive(Debug, Display, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Aggregation {
    Value,
    Max,
    Min,
    Sum,
    Avg,
}

impl Aggregation {
    pub fn combinator(&self) -> Box<dyn TelemetryCombinator> {
        match self {
            Self::Value => Box::new(combine::First),
            Self::Max => Box::new(combine::Max),
            Self::Min => Box::new(combine::Min),
            Self::Sum => Box::new(combine::Sum),
            Self::Avg => Box::new(combine::Average),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::phases::sense::flink::metric_order::MetricSpec;
    use crate::phases::sense::flink::{Aggregation, MetricOrder};
    use claim::*;
    use pretty_assertions::assert_eq;
    use proctor::elements::TelemetryType;
    use serde_json::json;
    use serde_test::{assert_tokens, Token};
    use trim_margin::MarginTrimmable;

    #[test]
    fn test_metric_order_serde_tokens() {
        let data_job = MetricOrder::Job(MetricSpec {
            metric: "lastCheckpointDuration".into(),
            agg: Aggregation::Max,
            telemetry_path: "health.last_checkpoint_duration".into(),
            telemetry_type: TelemetryType::Float,
        });

        assert_tokens(
            &data_job,
            &[
                Token::Map { len: Some(5) },
                Token::Str("scope"),
                Token::UnitVariant { name: "FlinkScope", variant: "Job" },
                Token::Str("metric"),
                Token::Str("lastCheckpointDuration"),
                Token::Str("agg"),
                Token::UnitVariant { name: "Aggregation", variant: "max" },
                Token::Str("telemetry_path"),
                Token::Str("health.last_checkpoint_duration"),
                Token::Str("telemetry_type"),
                Token::UnitVariant { name: "TelemetryType", variant: "Float" },
                Token::MapEnd,
            ],
        );

        let data_operator = MetricOrder::Operator(
            "Source: Data Stream".into(),
            MetricSpec {
                metric: "records-lag-max".into(),
                agg: Aggregation::Sum,
                telemetry_path: "flow.input_records_lag_max".into(),
                telemetry_type: TelemetryType::Integer,
            },
        );

        assert_tokens(
            &data_operator,
            &[
                Token::Map { len: Some(6) },
                Token::Str("scope"),
                Token::UnitVariant { name: "FlinkScope", variant: "Operator" },
                Token::Str("name"),
                Token::Str("Source: Data Stream"),
                Token::Str("metric"),
                Token::Str("records-lag-max"),
                Token::Str("agg"),
                Token::UnitVariant { name: "Aggregation", variant: "sum" },
                Token::Str("telemetry_path"),
                Token::Str("flow.input_records_lag_max"),
                Token::Str("telemetry_type"),
                Token::UnitVariant { name: "TelemetryType", variant: "Integer" },
                Token::MapEnd,
            ],
        );
    }

    #[test]
    fn test_metric_order_json_deser() {
        let data = json!([
            {
                "scope": "Job",
                "metric": "lastCheckpointDuration",
                "agg": "max",
                "telemetry_path": "health.last_checkpoint_duration",
                "telemetry_type": "Float"
            },
            {
                "scope": "Operator",
                "name": "Source: Data Stream",
                "metric": "records-lag-max",
                "agg": "sum",
                "telemetry_path": "flow.input_records_lag_max",
                "telemetry_type": "Integer"
            },
            {
                "scope": "Operator",
                "scope_name": "Source: Supplement Stream",
                "metric": "records-lag-max",
                "agg": "sum",
                "telemetry_path": "supplement.input_records_lag_max",
                "telemetry_type": "Integer"
            }
        ]);
        let actual: Vec<MetricOrder> = assert_ok!(serde_json::from_value(data));
        assert_eq!(
            actual,
            vec![
                MetricOrder::Job(MetricSpec {
                    metric: "lastCheckpointDuration".into(),
                    agg: Aggregation::Max,
                    telemetry_path: "health.last_checkpoint_duration".into(),
                    telemetry_type: TelemetryType::Float,
                }),
                MetricOrder::Operator(
                    "Source: Data Stream".into(),
                    MetricSpec {
                        metric: "records-lag-max".into(),
                        agg: Aggregation::Sum,
                        telemetry_path: "flow.input_records_lag_max".into(),
                        telemetry_type: TelemetryType::Integer,
                    }
                ),
                MetricOrder::Operator(
                    "Source: Supplement Stream".into(),
                    MetricSpec {
                        metric: "records-lag-max".into(),
                        agg: Aggregation::Sum,
                        telemetry_path: "supplement.input_records_lag_max".into(),
                        telemetry_type: TelemetryType::Integer,
                    }
                )
            ]
        );
    }

    #[test]
    fn test_metric_order_json_ser() {
        let data = vec![
            MetricOrder::Job(MetricSpec {
                metric: "lastCheckpointDuration".into(),
                agg: Aggregation::Max,
                telemetry_path: "health.last_checkpoint_duration".into(),
                telemetry_type: TelemetryType::Float,
            }),
            MetricOrder::Operator(
                "Source: Data Stream".into(),
                MetricSpec {
                    metric: "records-lag-max".into(),
                    agg: Aggregation::Sum,
                    telemetry_path: "flow.input_records_lag_max".into(),
                    telemetry_type: TelemetryType::Integer,
                },
            ),
            MetricOrder::Operator(
                "Source: Supplement Stream".into(),
                MetricSpec {
                    metric: "records-lag-max".into(),
                    agg: Aggregation::Sum,
                    telemetry_path: "supplement.input_records_lag_max".into(),
                    telemetry_type: TelemetryType::Integer,
                },
            ),
        ];

        let actual = assert_ok!(serde_json::to_string_pretty(&data));

        let expected = r##"|[
        |  {
        |    "scope": "Job",
        |    "metric": "lastCheckpointDuration",
        |    "agg": "max",
        |    "telemetry_path": "health.last_checkpoint_duration",
        |    "telemetry_type": "Float"
        |  },
        |  {
        |    "scope": "Operator",
        |    "name": "Source: Data Stream",
        |    "metric": "records-lag-max",
        |    "agg": "sum",
        |    "telemetry_path": "flow.input_records_lag_max",
        |    "telemetry_type": "Integer"
        |  },
        |  {
        |    "scope": "Operator",
        |    "name": "Source: Supplement Stream",
        |    "metric": "records-lag-max",
        |    "agg": "sum",
        |    "telemetry_path": "supplement.input_records_lag_max",
        |    "telemetry_type": "Integer"
        |  }
        |]"##
            .trim_margin_with("|")
            .unwrap();

        assert_eq!(actual, expected);
    }
}
