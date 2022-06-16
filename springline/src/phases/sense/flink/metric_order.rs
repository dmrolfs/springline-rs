use anyhow::anyhow;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Display;
use std::hash::Hash;

use itertools::Itertools;
use once_cell::sync::Lazy;
use proctor::elements::telemetry::combine::{self, TelemetryCombinator};
use proctor::elements::TelemetryType;
use proctor::error::SenseError;
use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::phases::sense::flink::STD_METRIC_ORDERS;
use crate::settings::FlinkSensorSettings;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeSpec {
    pub scope: FlinkScope,
    pub name: Option<String>,
}

impl ScopeSpec {
    pub fn new(scope: FlinkScope, name: impl Into<String>) -> Self {
        Self { scope, name: Some(name.into()) }
    }
}

impl From<FlinkScope> for ScopeSpec {
    fn from(scope: FlinkScope) -> Self {
        Self { scope, name: None }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MetricOrder {
    Job {
        #[serde(flatten)]
        spec: MetricSpec,
    },
    TaskManager {
        #[serde(flatten)]
        spec: MetricSpec,
    },
    Task {
        #[serde(flatten)]
        spec: MetricSpec,
    },
    Operator {
        name: String,
        #[serde(flatten)]
        spec: MetricSpec,
    },
}

pub type MetricOrderMatcher = Box<dyn Fn(&str) -> bool + Send + Sync + 'static>;


impl MetricOrder {
    pub fn new(scope: ScopeSpec, metric: MetricSpec) -> Result<Self, SenseError> {
        match scope.scope {
            FlinkScope::Job => Ok(Self::Job { spec: metric }),
            FlinkScope::TaskManager => Ok(Self::TaskManager { spec: metric }),
            FlinkScope::Task => Ok(Self::Task { spec: metric }),
            FlinkScope::Operator if scope.name.is_some() => {
                Ok(Self::Operator { name: scope.name.unwrap(), spec: metric })
            },
            _ => Err(SenseError::Stage(anyhow!(
                "bad metric order scope specification: {:?}",
                scope
            ))),
        }
    }

    pub fn matcher(&self) -> Result<MetricOrderMatcher, SenseError> {
        let matcher: MetricOrderMatcher = match self {
            Self::Job { spec } => {
                let match_metric = spec.metric.clone();
                Box::new(move |flink_metric| flink_metric == match_metric.as_str())
            },
            Self::TaskManager { spec } => {
                let match_metric = spec.metric.clone();
                Box::new(move |flink_metric| flink_metric == match_metric.as_str())
            },
            Self::Task { spec } => {
                let match_metric = spec.metric.clone();
                Box::new(move |flink_metric| flink_metric == match_metric.as_str())
            },
            Self::Operator { name, spec } => {
                let encoded_name = Self::encode_string(name);
                let regex = Regex::new(&format!(r"^{encoded_name}\..+\..*{metric}$", metric = spec.metric))
                    .map_err(|err| SenseError::Stage(err.into()))?;

                tracing::warn!(%encoded_name, ?regex, "DMR: MATCHER FOR OPERATOR [{name}]::[{}]", spec.metric);
                Box::new(move |flink_metric| regex.is_match(flink_metric))
            },
        };

        Ok(matcher)
    }

    fn encode_string(rep: &str) -> Cow<'_, str> {
        let mut escaped = String::with_capacity(rep.len() | 15);
        let unmodified = Self::append_string(rep.as_bytes(), &mut escaped, true);
        if unmodified {
            return Cow::Borrowed(rep);
        }
        Cow::Owned(escaped)
    }

    fn append_string(data: &[u8], escaped: &mut String, may_skip: bool) -> bool {
        let unmodified = Self::encode_into(data, may_skip, |s| {
            escaped.push_str(s);
            Ok::<_, std::convert::Infallible>(())
            // Ok::<_, std::convert::Infallible>(escaped.push_str(s))
        });

        if let Err(ref err) = unmodified {
            tracing::error!(error=?err, "failed to encode metric");
        }

        unmodified.unwrap()
    }

    fn encode_into<E>(
        mut data: &[u8], may_skip_write: bool, mut push_str: impl FnMut(&str) -> Result<(), E>,
    ) -> Result<bool, E> {
        let mut pushed = false;
        loop {
            // Fast path to skip over safe chars at the beginning of the remaining string
            let ascii_len = data
                .iter()
                .take_while(|&&c| matches!(c, b'0'..=b'9' | b'A'..=b'Z' | b'a'..=b'z' | b'-' | b'.' | b'_' | b'~'))
                .count();

            let (safe, rest) = if data.len() <= ascii_len {
                if !pushed && may_skip_write {
                    return Ok(true);
                }

                (data, &[][..]) // redundant to optimize out a panic in split_at
            } else {
                data.split_at(ascii_len)
            };

            pushed = true;

            if !safe.is_empty() {
                push_str(std::str::from_utf8(safe).unwrap())?;
            }
            if rest.is_empty() {
                break;
            }

            match rest.split_first() {
                Some((_eaten_byte, rest)) => {
                    let enc = &[b'_'];
                    push_str(std::str::from_utf8(enc).unwrap())?;
                    data = rest;
                },
                None => break,
            };
        }

        Ok(false)
    }

    pub const fn scope(&self) -> FlinkScope {
        match self {
            Self::Job { .. } => FlinkScope::Job,
            Self::TaskManager { .. } => FlinkScope::TaskManager,
            Self::Task { .. } => FlinkScope::Task,
            Self::Operator { .. } => FlinkScope::Operator,
        }
    }

    pub fn scope_name(&self) -> Option<&str> {
        match self {
            Self::Operator { name, .. } => Some(name.as_str()),
            _ => None,
        }
    }

    pub fn metric(&self) -> &str {
        match self {
            Self::Job { spec } => spec.metric.as_str(),
            Self::TaskManager { spec } => spec.metric.as_str(),
            Self::Task { spec } => spec.metric.as_str(),
            Self::Operator { spec, .. } => spec.metric.as_str(),
        }
    }

    pub const fn agg(&self) -> Aggregation {
        match self {
            Self::Job { spec } => spec.agg,
            Self::TaskManager { spec } => spec.agg,
            Self::Task { spec } => spec.agg,
            Self::Operator { spec, .. } => spec.agg,
        }
    }

    pub fn metric_path(&self) -> String {
        match self {
            Self::Job { spec } => format!("Job::{}", spec.metric),
            Self::TaskManager { spec } => format!("TaskManager::{}", spec.metric),
            Self::Task { spec } => format!("Task::{}", spec.metric),
            Self::Operator { name, spec } => format!("Operator::{name}::{}", spec.metric),
        }
    }

    pub fn telemetry(&self) -> (TelemetryType, &str) {
        match self {
            Self::Job { spec } => (spec.telemetry_type, spec.telemetry_path.as_str()),
            Self::TaskManager { spec } => (spec.telemetry_type, spec.telemetry_path.as_str()),
            Self::Task { spec } => (spec.telemetry_type, spec.telemetry_path.as_str()),
            Self::Operator { spec, .. } => (spec.telemetry_type, spec.telemetry_path.as_str()),
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

// impl Serialize for MetricOrder {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: Serializer,
//     {
//         let (variant, variant_index, size) = match self {
//             Self::Job { .. } => ("Job", 0, 4),
//             Self::TaskManager { .. } => ("TaskManager", 1, 4),
//             Self::Task { .. } => ("Task", 2, 4),
//             Self::Operator { .. } => ("Operator", 3, 5),
//         };
//
//         let mut state = serializer.serialize_struct_variant("MetricOrder", variant_index, variant, size)?;
//         state.serialize_field("scope", &self.scope())?;
//         if let Some(name) = self.scope_name() {
//             state.serialize_field("name", name)?;
//         }
//         state.serialize_field("metric", &self.metric())?;
//         state.serialize_field("agg", &self.agg())?;
//         let (telemetry_type, telemetry_path) = self.telemetry();
//         state.serialize_field("telemetry_path", telemetry_path)?;
//         state.serialize_field("telemetry_type", &telemetry_type)?;
//         state.end()
//     }
// }

// impl<'de> Deserialize<'de> for MetricOrder {
//     #[tracing::instrument(level = "trace", skip(deserializer))]
//     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
//     where
//         D: Deserializer<'de>,
//     {
//         #[derive(Deserialize)]
//         #[serde(field_identifier, rename_all = "snake_case")]
//         enum Field {
//             Scope,
//             #[serde(alias = "name")]
//             ScopeName,
//             Metric,
//             Agg,
//             TelemetryPath,
//             TelemetryType,
//         }
//
//         struct OrderVisitor;
//
//         impl<'de> Visitor<'de> for OrderVisitor {
//             type Value = MetricOrder;
//
//             fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//                 f.write_str("enum MetricOrder")
//             }
//
//             #[tracing::instrument(level = "trace", skip())]
//
//
//             #[tracing::instrument(level = "trace", skip(self, map))]
//             fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
//                 where A: EnumAccess<'de>,
//             {
//                 let mut scope = None;
//                 let mut scope_name = None;
//                 let mut metric = None;
//                 let mut agg = None;
//                 let mut telemetry_path = None;
//                 let mut telemetry_type = None;
//
//                 while let Some(key) = map.next_key()? {
//                     match key {
//                         Field::Scope => {
//                             if scope.is_some() {
//                                 return Err(de::Error::duplicate_field("scope"));
//                             }
//                             scope = Some(map.next_value()?);
//                         },
//                         Field::ScopeName => {
//                             if scope_name.is_some() {
//                                 return Err(de::Error::duplicate_field("name"));
//                             }
//                             scope_name = Some(map.next_value()?);
//                         },
//                         Field::Metric => {
//                             if metric.is_some() {
//                                 return Err(de::Error::duplicate_field("metric"));
//                             }
//                             metric = Some(map.next_value()?);
//                         },
//                         Field::Agg => {
//                             if agg.is_some() {
//                                 return Err(de::Error::duplicate_field("agg"));
//                             }
//                             agg = Some(map.next_value()?);
//                         },
//                         Field::TelemetryPath => {
//                             if telemetry_path.is_some() {
//                                 return Err(de::Error::duplicate_field("telemetry_path"));
//                             }
//                             telemetry_path = Some(map.next_value()?);
//                         },
//                         Field::TelemetryType => {
//                             if telemetry_type.is_some() {
//                                 return Err(de::Error::duplicate_field("telemetry_type"));
//                             }
//                             telemetry_type = Some(map.next_value()?);
//                         },
//                     }
//                 }
//
//                 let scope = scope.ok_or_else(|| de::Error::missing_field("scope"))?;
//                 let metric = metric.ok_or_else(|| de::Error::missing_field("metric"))?;
//                 let agg = agg.ok_or_else(|| de::Error::missing_field("agg"))?;
//                 let telemetry_path = telemetry_path.ok_or_else(|| de::Error::missing_field("telemetry_path"))?;
//                 let telemetry_type = telemetry_type.ok_or_else(|| de::Error::missing_field("telemetry_type"))?;
//
//                 let scope_spec = ScopeSpec { scope, name: scope_name };
//                 let metric_spec = MetricSpec { metric, agg, telemetry_path, telemetry_type };
//                 MetricOrder::from_scope_spec(scope_spec, metric_spec).map_err(de::Error::custom)
//             }
//
//             {
//                 todo!()
//             }
//         }
//
//         // const FIELDS: &'static [&'static str] = &["scope", "metric", "agg", "telemetry_path",
//         // "telemetry_type"];
//
//         deserializer.deserialize_map(OrderVisitor)
//     }
// }

#[derive(Debug, Display, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FlinkScope {
    Job,
    TaskManager,
    Task,
    Operator,
}

static FLINK_SCOPE_JOB: Lazy<String> = Lazy::new(|| FlinkScope::Job.to_string());
static FLINK_SCOPE_TASKMANAGER: Lazy<String> = Lazy::new(|| FlinkScope::TaskManager.to_string());
static FLINK_SCOPE_TASK: Lazy<String> = Lazy::new(|| FlinkScope::Task.to_string());
static FLINK_SCOPE_OPERATOR: Lazy<String> = Lazy::new(|| FlinkScope::Operator.to_string());

impl AsRef<str> for FlinkScope {
    fn as_ref(&self) -> &str {
        match self {
            Self::Job => FLINK_SCOPE_JOB.as_ref(),
            Self::Task => FLINK_SCOPE_TASK.as_ref(),
            Self::TaskManager => FLINK_SCOPE_TASKMANAGER.as_ref(),
            Self::Operator => FLINK_SCOPE_OPERATOR.as_ref(),
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
    use crate::phases::sense::flink::{Aggregation, MetricOrder, MetricSpec};
    use claim::*;
    use pretty_assertions::assert_eq;
    use proctor::elements::TelemetryType;
    use serde_json::json;
    use serde_test::{assert_tokens, Token};
    use trim_margin::MarginTrimmable;

    #[test]
    fn test_metric_order_serde_tokens() {
        let data_job = MetricOrder::Job {
            spec: MetricSpec {
                metric: "lastCheckpointDuration".into(),
                agg: Aggregation::Max,
                telemetry_path: "health.last_checkpoint_duration".into(),
                telemetry_type: TelemetryType::Float,
            },
        };

        assert_tokens(
            &data_job,
            &[
                Token::NewtypeVariant { name: "MetricOrder", variant: "Job" },
                Token::Map { len: None },
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

        let data_operator = MetricOrder::Operator {
            name: "Source: Data Stream".into(),
            spec: MetricSpec {
                metric: "records-lag-max".into(),
                agg: Aggregation::Sum,
                telemetry_path: "flow.input_records_lag_max".into(),
                telemetry_type: TelemetryType::Integer,
            },
        };

        assert_tokens(
            &data_operator,
            &[
                Token::NewtypeVariant { name: "MetricOrder", variant: "Operator" },
                Token::Map { len: None },
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
                "Job": {
                    "metric": "lastCheckpointDuration",
                    "agg": "max",
                    "telemetry_path": "health.last_checkpoint_duration",
                    "telemetry_type": "Float"
                }
            },
            {
                "Operator": {
                    "name": "Source: Data Stream",
                    "metric": "records-lag-max",
                    "agg": "sum",
                    "telemetry_path": "flow.input_records_lag_max",
                    "telemetry_type": "Integer"
                }
            },
            {
                "Operator": {
                    "name": "Source: Supplement Stream",
                    "metric": "records-lag-max",
                    "agg": "sum",
                    "telemetry_path": "supplement.input_records_lag_max",
                    "telemetry_type": "Integer"
                }
            }
        ]);
        let actual: Vec<MetricOrder> = assert_ok!(serde_json::from_value(data));
        assert_eq!(
            actual,
            vec![
                MetricOrder::Job {
                    spec: MetricSpec {
                        metric: "lastCheckpointDuration".into(),
                        agg: Aggregation::Max,
                        telemetry_path: "health.last_checkpoint_duration".into(),
                        telemetry_type: TelemetryType::Float,
                    }
                },
                MetricOrder::Operator {
                    name: "Source: Data Stream".into(),
                    spec: MetricSpec {
                        metric: "records-lag-max".into(),
                        agg: Aggregation::Sum,
                        telemetry_path: "flow.input_records_lag_max".into(),
                        telemetry_type: TelemetryType::Integer,
                    }
                },
                MetricOrder::Operator {
                    name: "Source: Supplement Stream".into(),
                    spec: MetricSpec {
                        metric: "records-lag-max".into(),
                        agg: Aggregation::Sum,
                        telemetry_path: "supplement.input_records_lag_max".into(),
                        telemetry_type: TelemetryType::Integer,
                    }
                },
            ]
        );
    }

    #[test]
    fn test_metric_order_json_ser() {
        let data = vec![
            MetricOrder::Job {
                spec: MetricSpec {
                    metric: "lastCheckpointDuration".into(),
                    agg: Aggregation::Max,
                    telemetry_path: "health.last_checkpoint_duration".into(),
                    telemetry_type: TelemetryType::Float,
                },
            },
            MetricOrder::Operator {
                name: "Source: Data Stream".into(),
                spec: MetricSpec {
                    metric: "records-lag-max".into(),
                    agg: Aggregation::Sum,
                    telemetry_path: "flow.input_records_lag_max".into(),
                    telemetry_type: TelemetryType::Integer,
                },
            },
            MetricOrder::Operator {
                name: "Source: Supplement Stream".into(),
                spec: MetricSpec {
                    metric: "records-lag-max".into(),
                    agg: Aggregation::Sum,
                    telemetry_path: "supplement.input_records_lag_max".into(),
                    telemetry_type: TelemetryType::Integer,
                },
            },
        ];

        let actual = assert_ok!(serde_json::to_string_pretty(&data));

        let expected = r##"|[
        |  {
        |    "Job": {
        |      "metric": "lastCheckpointDuration",
        |      "agg": "max",
        |      "telemetry_path": "health.last_checkpoint_duration",
        |      "telemetry_type": "Float"
        |    }
        |  },
        |  {
        |    "Operator": {
        |      "name": "Source: Data Stream",
        |      "metric": "records-lag-max",
        |      "agg": "sum",
        |      "telemetry_path": "flow.input_records_lag_max",
        |      "telemetry_type": "Integer"
        |    }
        |  },
        |  {
        |    "Operator": {
        |      "name": "Source: Supplement Stream",
        |      "metric": "records-lag-max",
        |      "agg": "sum",
        |      "telemetry_path": "supplement.input_records_lag_max",
        |      "telemetry_type": "Integer"
        |    }
        |  }
        |]"##
            .trim_margin_with("|")
            .unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_metric_order_yaml_deser() {
        let data = r##"|---
        |- Job:
        |    metric: lastCheckpointDuration
        |    agg: max
        |    telemetry_path: health.last_checkpoint_duration
        |    telemetry_type: Float
        |- Operator:
        |    name: "Source: Data Stream"
        |    metric: records-lag-max
        |    agg: sum
        |    telemetry_path: flow.input_records_lag_max
        |    telemetry_type: Integer
        |- Operator:
        |    name: "Source: Supplement Stream"
        |    metric: records-lag-max
        |    agg: sum
        |    telemetry_path: supplement.input_records_lag_max
        |    telemetry_type: Integer
        |"##
        .trim_margin_with("|")
        .unwrap();

        let actual: Vec<MetricOrder> = assert_ok!(serde_yaml::from_str(&data));
        assert_eq!(
            actual,
            vec![
                MetricOrder::Job {
                    spec: MetricSpec {
                        metric: "lastCheckpointDuration".into(),
                        agg: Aggregation::Max,
                        telemetry_path: "health.last_checkpoint_duration".into(),
                        telemetry_type: TelemetryType::Float,
                    }
                },
                MetricOrder::Operator {
                    name: "Source: Data Stream".into(),
                    spec: MetricSpec {
                        metric: "records-lag-max".into(),
                        agg: Aggregation::Sum,
                        telemetry_path: "flow.input_records_lag_max".into(),
                        telemetry_type: TelemetryType::Integer,
                    }
                },
                MetricOrder::Operator {
                    name: "Source: Supplement Stream".into(),
                    spec: MetricSpec {
                        metric: "records-lag-max".into(),
                        agg: Aggregation::Sum,
                        telemetry_path: "supplement.input_records_lag_max".into(),
                        telemetry_type: TelemetryType::Integer,
                    }
                },
            ]
        );
    }

    #[test]
    fn test_metric_order_yaml_ser() {
        let data = vec![
            MetricOrder::Job {
                spec: MetricSpec {
                    metric: "lastCheckpointDuration".into(),
                    agg: Aggregation::Max,
                    telemetry_path: "health.last_checkpoint_duration".into(),
                    telemetry_type: TelemetryType::Float,
                },
            },
            MetricOrder::Operator {
                name: "Source: Data Stream".into(),
                spec: MetricSpec {
                    metric: "records-lag-max".into(),
                    agg: Aggregation::Sum,
                    telemetry_path: "flow.input_records_lag_max".into(),
                    telemetry_type: TelemetryType::Integer,
                },
            },
            MetricOrder::Operator {
                name: "Source: Supplement Stream".into(),
                spec: MetricSpec {
                    metric: "records-lag-max".into(),
                    agg: Aggregation::Sum,
                    telemetry_path: "supplement.input_records_lag_max".into(),
                    telemetry_type: TelemetryType::Integer,
                },
            },
        ];

        let actual = assert_ok!(serde_yaml::to_string(&data));

        let expected = r##"|---
        |- Job:
        |    metric: lastCheckpointDuration
        |    agg: max
        |    telemetry_path: health.last_checkpoint_duration
        |    telemetry_type: Float
        |- Operator:
        |    name: "Source: Data Stream"
        |    metric: records-lag-max
        |    agg: sum
        |    telemetry_path: flow.input_records_lag_max
        |    telemetry_type: Integer
        |- Operator:
        |    name: "Source: Supplement Stream"
        |    metric: records-lag-max
        |    agg: sum
        |    telemetry_path: supplement.input_records_lag_max
        |    telemetry_type: Integer
        |"##
        .trim_margin_with("|")
        .unwrap();

        assert_eq!(actual, expected);
    }

    #[ignore = "ron deser doesn't support enum w flattened internal (map format) that it generates in ser"]
    #[test]
    fn test_metric_order_ron_deser() {
        let data = r##"|[
        |   Job({
        |        "metric": "lastCheckpointDuration",
        |        "agg": max,
        |        "telemetry_path": "health.last_checkpoint_duration",
        |        "telemetry_type": Float,
        |   ),
        |   Operator({
        |       "name": "Source: Data Stream",
        |       "metric": "records-lag-max",
        |       "agg": sum,
        |       "telemetry_path": "flow.input_records_lag_max",
        |       "telemetry_type": Integer,
        |   }),
        |   Operator({
        |       "name": "Source: Supplement Stream",
        |       "metric": "records-lag-max",
        |       "agg": sum,
        |       "telemetry_path": "supplement.input_records_lag_max",
        |       "telemetry_type": Integer,
        |   }),
        |]"##
            .trim_margin_with("|")
            .unwrap();

        let actual: Vec<MetricOrder> = assert_ok!(ron::from_str(&data));
        assert_eq!(
            actual,
            vec![
                MetricOrder::Job {
                    spec: MetricSpec {
                        metric: "lastCheckpointDuration".into(),
                        agg: Aggregation::Max,
                        telemetry_path: "health.last_checkpoint_duration".into(),
                        telemetry_type: TelemetryType::Float,
                    }
                },
                MetricOrder::Operator {
                    name: "Source: Data Stream".into(),
                    spec: MetricSpec {
                        metric: "records-lag-max".into(),
                        agg: Aggregation::Sum,
                        telemetry_path: "flow.input_records_lag_max".into(),
                        telemetry_type: TelemetryType::Integer,
                    }
                },
                MetricOrder::Operator {
                    name: "Source: Supplement Stream".into(),
                    spec: MetricSpec {
                        metric: "records-lag-max".into(),
                        agg: Aggregation::Sum,
                        telemetry_path: "supplement.input_records_lag_max".into(),
                        telemetry_type: TelemetryType::Integer,
                    }
                },
            ]
        );
    }

    #[test]
    fn test_metric_order_ron_ser() {
        let data = vec![
            MetricOrder::Job {
                spec: MetricSpec {
                    metric: "lastCheckpointDuration".into(),
                    agg: Aggregation::Max,
                    telemetry_path: "health.last_checkpoint_duration".into(),
                    telemetry_type: TelemetryType::Float,
                },
            },
            MetricOrder::Operator {
                name: "Source: Data Stream".into(),
                spec: MetricSpec {
                    metric: "records-lag-max".into(),
                    agg: Aggregation::Sum,
                    telemetry_path: "flow.input_records_lag_max".into(),
                    telemetry_type: TelemetryType::Integer,
                },
            },
            MetricOrder::Operator {
                name: "Source: Supplement Stream".into(),
                spec: MetricSpec {
                    metric: "records-lag-max".into(),
                    agg: Aggregation::Sum,
                    telemetry_path: "supplement.input_records_lag_max".into(),
                    telemetry_type: TelemetryType::Integer,
                },
            },
        ];

        let actual = assert_ok!(ron::ser::to_string_pretty(&data, ron::ser::PrettyConfig::default()));

        let expected = r##"|[
        |    Job({
        |        "metric": "lastCheckpointDuration",
        |        "agg": max,
        |        "telemetry_path": "health.last_checkpoint_duration",
        |        "telemetry_type": Float,
        |    }),
        |    Operator({
        |        "name": "Source: Data Stream",
        |        "metric": "records-lag-max",
        |        "agg": sum,
        |        "telemetry_path": "flow.input_records_lag_max",
        |        "telemetry_type": Integer,
        |    }),
        |    Operator({
        |        "name": "Source: Supplement Stream",
        |        "metric": "records-lag-max",
        |        "agg": sum,
        |        "telemetry_path": "supplement.input_records_lag_max",
        |        "telemetry_type": Integer,
        |    }),
        |]"##
            .trim_margin_with("|")
            .unwrap();

        assert_eq!(actual, expected);
    }
}
