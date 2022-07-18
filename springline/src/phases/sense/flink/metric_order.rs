// use anyhow::anyhow;
use std::borrow::Cow;
use std::fmt::Display;
use std::hash::Hash;

use once_cell::sync::Lazy;
use proctor::elements::telemetry::combine::{self, TelemetryCombinator};
use proctor::elements::{TelemetryType, TelemetryValue};
use proctor::error::{SenseError, TelemetryError};
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PlanPositionCandidate<'c> {
    Any,
    ByName(&'c str),
}

const FLINK_SOURCE_PREFIX: &str = "Source:";
const FLINK_SINK_PREFIX: &str = "Sink:";

#[derive(Debug, Display, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanPositionSpec {
    Any,
    Source,
    NotSource,
    Sink,
    NotSink,
    Through,
}

impl Default for PlanPositionSpec {
    fn default() -> Self {
        Self::Any
    }
}

impl PlanPositionSpec {
    pub const fn is_all(&self) -> bool {
        matches!(self, Self::Any)
    }

    pub fn matches<'c>(&self, candidate: &PlanPositionCandidate<'c>) -> bool {
        match (self, candidate) {
            (Self::Any, _) => true,
            (_, PlanPositionCandidate::Any) => true,
            (Self::Source, PlanPositionCandidate::ByName(name)) => name.starts_with(FLINK_SOURCE_PREFIX),
            (Self::NotSource, PlanPositionCandidate::ByName(name)) => !name.starts_with(FLINK_SOURCE_PREFIX),
            (Self::Sink, PlanPositionCandidate::ByName(name)) => name.contains(FLINK_SINK_PREFIX),
            (Self::NotSink, PlanPositionCandidate::ByName(name)) => !name.contains(FLINK_SINK_PREFIX),
            (Self::Through, PlanPositionCandidate::ByName(name)) => {
                !name.starts_with(FLINK_SOURCE_PREFIX) && !name.contains(FLINK_SINK_PREFIX)
            },
        }
    }
}

#[derive(Debug, Display, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DerivativeCombinator {
    Product,
}

impl DerivativeCombinator {
    #[tracing::instrument(level = "trace", name = "derivative_order_combine")]
    pub fn combine(&self, lhs: &TelemetryValue, rhs: &TelemetryValue) -> Result<TelemetryValue, TelemetryError> {
        let items = Self::do_normalize_types(lhs, rhs);
        let result = match self {
            Self::Product => combine::Product.combine(items.clone()).map(|v| v.unwrap()),
        };
        tracing::debug!("derivative_order_combine({:?}) = {:?}", items, result);
        result
    }

    fn do_normalize_types(lhs: &TelemetryValue, rhs: &TelemetryValue) -> Vec<TelemetryValue> {
        match (lhs.as_telemetry_type(), rhs.as_telemetry_type()) {
            (TelemetryType::Float, _) => vec![lhs.clone(), rhs.clone()],
            (_, TelemetryType::Float) => vec![rhs.clone(), lhs.clone()],
            (TelemetryType::Integer, _) => vec![lhs.clone(), rhs.clone()],
            (_, TelemetryType::Integer) => vec![rhs.clone(), lhs.clone()],
            _ => vec![lhs.clone(), rhs.clone()],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MetricOrder {
    Job {
        #[serde(flatten)]
        metric: MetricSpec,
    },
    TaskManager {
        #[serde(flatten)]
        metric: MetricSpec,
    },
    Task {
        #[serde(default, skip_serializing_if = "PlanPositionSpec::is_all")]
        position: PlanPositionSpec,
        #[serde(flatten)]
        metric: MetricSpec,
    },
    Operator {
        name: String,
        #[serde(default, skip_serializing_if = "PlanPositionSpec::is_all")]
        position: PlanPositionSpec,
        #[serde(flatten)]
        metric: MetricSpec,
    },
    Derivative {
        scope: FlinkScope,
        #[serde(default, skip_serializing_if = "PlanPositionSpec::is_all")]
        position: PlanPositionSpec,
        telemetry_path: String,
        telemetry_type: TelemetryType,
        telemetry_lhs: String,
        telemetry_rhs: String,
        combinator: DerivativeCombinator,
        agg: Aggregation,
    },
}

// #[derive(Debug, PartialEq, Eq)]
#[derive(Debug)]
pub struct MetricCandidate<'c> {
    pub metric: &'c str,
    pub position: PlanPositionCandidate<'c>,
}

pub type MetricOrderMatcher = Box<dyn for<'c> Fn(&MetricCandidate<'c>) -> bool + Send + Sync + 'static>;

impl MetricOrder {
    pub fn matches_plan_position(&self, candidate: &PlanPositionCandidate<'_>) -> bool {
        match self {
            Self::Job { .. } => true,
            Self::TaskManager { .. } => true,
            Self::Task { position, .. } => position.matches(candidate),
            Self::Operator { position, .. } => position.matches(candidate),
            Self::Derivative { position, .. } => position.matches(candidate),
        }
    }

    pub fn metric_matcher(&self) -> Result<MetricOrderMatcher, SenseError> {
        let matcher: MetricOrderMatcher = match self {
            Self::Job { metric: spec } => {
                let match_metric = spec.metric.clone();
                Box::new(move |candidate| candidate.metric == match_metric.as_str())
            },
            Self::TaskManager { metric: spec } => {
                let match_metric = spec.metric.clone();
                Box::new(move |candidate| candidate.metric == match_metric.as_str())
            },
            Self::Task { position, metric: spec } => {
                let pos_spec = *position;
                let match_metric = spec.metric.clone();
                Box::new(move |candidate| {
                    let pos_match = pos_spec.matches(&candidate.position);
                    pos_match && candidate.metric == match_metric.as_str()
                })
            },
            Self::Operator { name, position, metric: spec } => {
                let pos_spec = *position;
                let encoded_name = Self::encode_string(name);
                let regex = Regex::new(&format!(
                    r"^({encoded_source_prefix})?{encoded_name}.*\..*{metric}$",
                    encoded_source_prefix = Self::encode_string(&format!("{FLINK_SOURCE_PREFIX} ")),
                    metric = spec.metric
                ))
                .map_err(|err| SenseError::Stage(err.into()))?;

                // let dmr_name = name.clone();
                // let dmr_spec = spec.clone();
                Box::new(move |candidate| {
                    let pos_match = pos_spec.matches(&candidate.position);
                    // tracing::trace!(?candidate, "DMR: operator[{}]::[{}] position match = {pos_match}", dmr_name, dmr_spec.metric);
                    // let regex_match = regex.is_match(candidate.metric);
                    // tracing::warn!(?regex, candidate=%candidate.metric, "DMR: operator[{}]::[{}] regex match = {regex_match}", dmr_name, dmr_spec.metric);

                    pos_match && regex.is_match(candidate.metric)
                })
            },
            Self::Derivative { .. } => Box::new(|_| false), // creates and doesn't pull metrics
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
            Self::Derivative { scope, .. } => *scope,
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
            Self::Job { metric: spec } => spec.metric.as_str(),
            Self::TaskManager { metric: spec } => spec.metric.as_str(),
            Self::Task { metric: spec, .. } => spec.metric.as_str(),
            Self::Operator { metric: spec, .. } => spec.metric.as_str(),
            Self::Derivative { .. } => "",
        }
    }

    pub const fn agg(&self) -> Aggregation {
        match self {
            Self::Job { metric: spec } => spec.agg,
            Self::TaskManager { metric: spec } => spec.agg,
            Self::Task { metric: spec, .. } => spec.agg,
            Self::Operator { metric: spec, .. } => spec.agg,
            Self::Derivative { agg, .. } => *agg,
        }
    }

    // pub fn metric_path(&self) -> String {
    //     match self {
    //         Self::Job { metric: spec } => format!("Job::{}", spec.metric),
    //         Self::TaskManager { metric: spec } => format!("TaskManager::{}", spec.metric),
    //         Self::Task { metric: spec, .. } => format!("Task::{}", spec.metric),
    //         Self::Operator { name, metric: spec, .. } => format!("Operator::{name}::{}", spec.metric),
    //     }
    // }

    pub fn telemetry(&self) -> (TelemetryType, &str) {
        match self {
            Self::Job { metric: spec } => (spec.telemetry_type, spec.telemetry_path.as_str()),
            Self::TaskManager { metric: spec } => (spec.telemetry_type, spec.telemetry_path.as_str()),
            Self::Task { metric: spec, .. } => (spec.telemetry_type, spec.telemetry_path.as_str()),
            Self::Operator { metric: spec, .. } => (spec.telemetry_type, spec.telemetry_path.as_str()),
            Self::Derivative { telemetry_type, telemetry_path, .. } => (*telemetry_type, telemetry_path.as_str()),
        }
    }

    pub fn extend_standard_with_settings(settings: &FlinkSensorSettings) -> Vec<Self> {
        let mut orders = STD_METRIC_ORDERS.clone();
        orders.extend(settings.metric_orders.clone());
        orders
    }

    // pub fn organize_by_scope(orders: &[Self]) -> HashMap<FlinkScope, Vec<Self>> {
    //     let mut result: HashMap<FlinkScope, Vec<Self>> = HashMap::default();
    //
    //     for (scope, group) in &orders.iter().group_by(|o| o.scope()) {
    //         let orders = result.entry(scope).or_insert_with(Vec::new);
    //         let group: Vec<Self> = group.cloned().collect();
    //         orders.extend(group);
    //     }
    //
    //     result
    // }
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
#[serde(rename_all = "snake_case")]
pub enum FlinkScope {
    Job,
    //todo: JobManager,
    TaskManager,
    Task,
    Operator,
    Any,
}

impl FlinkScope {
    pub fn matches(&self, other: &FlinkScope) -> bool {
        *self == Self::Any || *other == Self::Any || *self == *other
    }
}

static FLINK_SCOPE_JOB: Lazy<String> = Lazy::new(|| FlinkScope::Job.to_string());
static FLINK_SCOPE_TASKMANAGER: Lazy<String> = Lazy::new(|| FlinkScope::TaskManager.to_string());
static FLINK_SCOPE_TASK: Lazy<String> = Lazy::new(|| FlinkScope::Task.to_string());
static FLINK_SCOPE_OPERATOR: Lazy<String> = Lazy::new(|| FlinkScope::Operator.to_string());
static FLINK_SCOPE_ANY: Lazy<String> = Lazy::new(|| FlinkScope::Any.to_string());

impl AsRef<str> for FlinkScope {
    fn as_ref(&self) -> &str {
        match self {
            Self::Job => FLINK_SCOPE_JOB.as_ref(),
            Self::Task => FLINK_SCOPE_TASK.as_ref(),
            Self::TaskManager => FLINK_SCOPE_TASKMANAGER.as_ref(),
            Self::Operator => FLINK_SCOPE_OPERATOR.as_ref(),
            Self::Any => FLINK_SCOPE_ANY.as_ref(),
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
    use crate::phases::sense::flink::metric_order::MetricCandidate;
    use crate::phases::sense::flink::{Aggregation, MetricOrder, MetricSpec, PlanPositionCandidate, PlanPositionSpec};
    use claim::*;
    use pretty_assertions::assert_eq;
    use proctor::elements::TelemetryType;
    use serde_json::json;
    use serde_test::{assert_tokens, Token};
    use std::collections::HashSet;
    use std::hash::{Hash, Hasher};
    use trim_margin::MarginTrimmable;

    #[test]
    fn test_operator_order_matches() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_operator_order_matches");
        let _ = main_span.enter();

        let order = MetricOrder::Operator {
            name: "FOO Data stream".to_string(),
            position: PlanPositionSpec::Source,
            metric: MetricSpec::new(
                "records-lag-max",
                Aggregation::Sum,
                "flow.source_records_lag_max",
                TelemetryType::Integer,
            ),
        };
        let order_matcher = assert_ok!(order.metric_matcher());

        let pos_candidate = PlanPositionCandidate::ByName(
            "Source: FOO Data stream -> Processing partitions to stream. -> Filtering records that fail score check. -> Indexing source data.."
        );

        let metric_candidate = MetricCandidate {
            metric: "Source__FOO_Data_stream.KafkaConsumer.client-id.18b9e5dd74ea45fcb2397212225b8dd2.topic.zed-xyz-abc-ingestion-service-stg.layer.observations.consumer-fetch-manager-metrics_records-lag-max",
            position: pos_candidate,
        };

        assert!(order_matcher(&metric_candidate));
    }

    #[test]
    fn test_aggregation_hash_eq() {
        fn test_scenario(lhs: Aggregation, rhs: Aggregation, same: bool) {
            let mut lhs_hasher = std::collections::hash_map::DefaultHasher::new();
            lhs.hash(&mut lhs_hasher);
            let mut rhs_hasher = std::collections::hash_map::DefaultHasher::new();
            rhs.hash(&mut rhs_hasher);
            if same {
                assert_eq!(lhs_hasher.finish(), rhs_hasher.finish());
                assert_eq!(lhs, rhs);
            } else {
                assert_ne!(lhs_hasher.finish(), rhs_hasher.finish());
                assert_ne!(lhs, rhs);
            }
        }

        test_scenario(Aggregation::Value, Aggregation::Value, true);
        test_scenario(Aggregation::Value, Aggregation::Max, false);
        test_scenario(Aggregation::Value, Aggregation::Min, false);
        test_scenario(Aggregation::Value, Aggregation::Sum, false);
        test_scenario(Aggregation::Value, Aggregation::Avg, false);

        test_scenario(Aggregation::Max, Aggregation::Value, false);
        test_scenario(Aggregation::Max, Aggregation::Max, true);
        test_scenario(Aggregation::Max, Aggregation::Min, false);
        test_scenario(Aggregation::Max, Aggregation::Sum, false);
        test_scenario(Aggregation::Max, Aggregation::Avg, false);

        test_scenario(Aggregation::Min, Aggregation::Value, false);
        test_scenario(Aggregation::Min, Aggregation::Max, false);
        test_scenario(Aggregation::Min, Aggregation::Min, true);
        test_scenario(Aggregation::Min, Aggregation::Sum, false);
        test_scenario(Aggregation::Min, Aggregation::Avg, false);

        test_scenario(Aggregation::Sum, Aggregation::Value, false);
        test_scenario(Aggregation::Sum, Aggregation::Max, false);
        test_scenario(Aggregation::Sum, Aggregation::Min, false);
        test_scenario(Aggregation::Sum, Aggregation::Sum, true);
        test_scenario(Aggregation::Sum, Aggregation::Avg, false);

        test_scenario(Aggregation::Avg, Aggregation::Value, false);
        test_scenario(Aggregation::Avg, Aggregation::Max, false);
        test_scenario(Aggregation::Avg, Aggregation::Min, false);
        test_scenario(Aggregation::Avg, Aggregation::Sum, false);
        test_scenario(Aggregation::Avg, Aggregation::Avg, true);

        let data = vec![
            Aggregation::Max,
            Aggregation::Max,
            Aggregation::Sum,
            Aggregation::Avg,
            Aggregation::Max,
        ];
        let actual: HashSet<Aggregation> = data.iter().copied().collect();
        assert_eq!(
            actual,
            maplit::hashset! {Aggregation::Max, Aggregation::Sum, Aggregation::Avg,}
        );
    }

    #[test]
    fn test_metric_order_serde_tokens() {
        let data_job = MetricOrder::Job {
            metric: MetricSpec {
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
            metric: MetricSpec {
                metric: "records-lag-max".into(),
                agg: Aggregation::Sum,
                telemetry_path: "flow.source_records_lag_max".into(),
                telemetry_type: TelemetryType::Integer,
            },
            position: PlanPositionSpec::NotSource,
        };

        assert_tokens(
            &data_operator,
            &[
                Token::NewtypeVariant { name: "MetricOrder", variant: "Operator" },
                Token::Map { len: None },
                Token::Str("name"),
                Token::Str("Source: Data Stream"),
                Token::Str("position"),
                Token::UnitVariant { name: "PlanPositionSpec", variant: "not_source" },
                Token::Str("metric"),
                Token::Str("records-lag-max"),
                Token::Str("agg"),
                Token::UnitVariant { name: "Aggregation", variant: "sum" },
                Token::Str("telemetry_path"),
                Token::Str("flow.source_records_lag_max"),
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
                    "position": "source",
                    "metric": "records-lag-max",
                    "agg": "sum",
                    "telemetry_path": "flow.source_records_lag_max",
                    "telemetry_type": "Integer"
                }
            },
            {
                "Operator": {
                    "name": "Source: Supplement Stream",
                    "position": "source",
                    "metric": "records-lag-max",
                    "agg": "sum",
                    "telemetry_path": "supplement.source_records_lag_max",
                    "telemetry_type": "Integer"
                }
            }
        ]);
        let actual: Vec<MetricOrder> = assert_ok!(serde_json::from_value(data));
        assert_eq!(
            actual,
            vec![
                MetricOrder::Job {
                    metric: MetricSpec {
                        metric: "lastCheckpointDuration".into(),
                        agg: Aggregation::Max,
                        telemetry_path: "health.last_checkpoint_duration".into(),
                        telemetry_type: TelemetryType::Float,
                    }
                },
                MetricOrder::Operator {
                    name: "Source: Data Stream".into(),
                    metric: MetricSpec {
                        metric: "records-lag-max".into(),
                        agg: Aggregation::Sum,
                        telemetry_path: "flow.source_records_lag_max".into(),
                        telemetry_type: TelemetryType::Integer,
                    },
                    position: PlanPositionSpec::Source,
                },
                MetricOrder::Operator {
                    name: "Source: Supplement Stream".into(),
                    metric: MetricSpec {
                        metric: "records-lag-max".into(),
                        agg: Aggregation::Sum,
                        telemetry_path: "supplement.source_records_lag_max".into(),
                        telemetry_type: TelemetryType::Integer,
                    },
                    position: PlanPositionSpec::Source,
                },
            ]
        );
    }

    #[test]
    fn test_metric_order_json_ser() {
        let data = vec![
            MetricOrder::Job {
                metric: MetricSpec {
                    metric: "lastCheckpointDuration".into(),
                    agg: Aggregation::Max,
                    telemetry_path: "health.last_checkpoint_duration".into(),
                    telemetry_type: TelemetryType::Float,
                },
            },
            MetricOrder::Operator {
                name: "Source: Data Stream".into(),
                metric: MetricSpec {
                    metric: "records-lag-max".into(),
                    agg: Aggregation::Sum,
                    telemetry_path: "flow.source_records_lag_max".into(),
                    telemetry_type: TelemetryType::Integer,
                },
                position: PlanPositionSpec::Source,
            },
            MetricOrder::Operator {
                name: "Supplement Stream".into(),
                metric: MetricSpec {
                    metric: "records-lag-max".into(),
                    agg: Aggregation::Sum,
                    telemetry_path: "supplement.source_records_lag_max".into(),
                    telemetry_type: TelemetryType::Integer,
                },
                position: PlanPositionSpec::Any,
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
        |      "position": "source",
        |      "metric": "records-lag-max",
        |      "agg": "sum",
        |      "telemetry_path": "flow.source_records_lag_max",
        |      "telemetry_type": "Integer"
        |    }
        |  },
        |  {
        |    "Operator": {
        |      "name": "Supplement Stream",
        |      "metric": "records-lag-max",
        |      "agg": "sum",
        |      "telemetry_path": "supplement.source_records_lag_max",
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
        |    telemetry_path: flow.source_records_lag_max
        |    telemetry_type: Integer
        |- Operator:
        |    name: "Supplement Stream"
        |    position: not_source
        |    metric: records-lag-max
        |    agg: sum
        |    telemetry_path: supplement.source_records_lag_max
        |    telemetry_type: Integer
        |"##
        .trim_margin_with("|")
        .unwrap();

        let actual: Vec<MetricOrder> = assert_ok!(serde_yaml::from_str(&data));
        assert_eq!(
            actual,
            vec![
                MetricOrder::Job {
                    metric: MetricSpec {
                        metric: "lastCheckpointDuration".into(),
                        agg: Aggregation::Max,
                        telemetry_path: "health.last_checkpoint_duration".into(),
                        telemetry_type: TelemetryType::Float,
                    }
                },
                MetricOrder::Operator {
                    name: "Source: Data Stream".into(),
                    metric: MetricSpec {
                        metric: "records-lag-max".into(),
                        agg: Aggregation::Sum,
                        telemetry_path: "flow.source_records_lag_max".into(),
                        telemetry_type: TelemetryType::Integer,
                    },
                    position: PlanPositionSpec::Any,
                },
                MetricOrder::Operator {
                    name: "Supplement Stream".into(),
                    metric: MetricSpec {
                        metric: "records-lag-max".into(),
                        agg: Aggregation::Sum,
                        telemetry_path: "supplement.source_records_lag_max".into(),
                        telemetry_type: TelemetryType::Integer,
                    },
                    position: PlanPositionSpec::NotSource,
                },
            ]
        );
    }

    #[test]
    fn test_metric_order_yaml_ser() {
        let data = vec![
            MetricOrder::Job {
                metric: MetricSpec {
                    metric: "lastCheckpointDuration".into(),
                    agg: Aggregation::Max,
                    telemetry_path: "health.last_checkpoint_duration".into(),
                    telemetry_type: TelemetryType::Float,
                },
            },
            MetricOrder::Operator {
                name: "Data Stream".into(),
                metric: MetricSpec {
                    metric: "records-lag-max".into(),
                    agg: Aggregation::Sum,
                    telemetry_path: "flow.source_records_lag_max".into(),
                    telemetry_type: TelemetryType::Integer,
                },
                position: PlanPositionSpec::Any,
            },
            MetricOrder::Operator {
                name: "Supplement Stream".into(),
                metric: MetricSpec {
                    metric: "records-lag-max".into(),
                    agg: Aggregation::Sum,
                    telemetry_path: "supplement.source_records_lag_max".into(),
                    telemetry_type: TelemetryType::Integer,
                },
                position: PlanPositionSpec::NotSource,
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
        |    name: Data Stream
        |    metric: records-lag-max
        |    agg: sum
        |    telemetry_path: flow.source_records_lag_max
        |    telemetry_type: Integer
        |- Operator:
        |    name: Supplement Stream
        |    position: not_source
        |    metric: records-lag-max
        |    agg: sum
        |    telemetry_path: supplement.source_records_lag_max
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
        |       "telemetry_path": "flow.source_records_lag_max",
        |       "telemetry_type": Integer,
        |   }),
        |   Operator({
        |       "name": "Source: Supplement Stream",
        |       "position": source,
        |       "metric": "records-lag-max",
        |       "agg": sum,
        |       "telemetry_path": "supplement.source_records_lag_max",
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
                    metric: MetricSpec {
                        metric: "lastCheckpointDuration".into(),
                        agg: Aggregation::Max,
                        telemetry_path: "health.last_checkpoint_duration".into(),
                        telemetry_type: TelemetryType::Float,
                    }
                },
                MetricOrder::Operator {
                    name: "Source: Data Stream".into(),
                    metric: MetricSpec {
                        metric: "records-lag-max".into(),
                        agg: Aggregation::Sum,
                        telemetry_path: "flow.source_records_lag_max".into(),
                        telemetry_type: TelemetryType::Integer,
                    },
                    position: PlanPositionSpec::Any,
                },
                MetricOrder::Operator {
                    name: "Source: Supplement Stream".into(),
                    metric: MetricSpec {
                        metric: "records-lag-max".into(),
                        agg: Aggregation::Sum,
                        telemetry_path: "supplement.source_records_lag_max".into(),
                        telemetry_type: TelemetryType::Integer,
                    },
                    position: PlanPositionSpec::Source,
                },
            ]
        );
    }

    #[test]
    fn test_metric_order_ron_ser() {
        let data = vec![
            MetricOrder::Job {
                metric: MetricSpec {
                    metric: "lastCheckpointDuration".into(),
                    agg: Aggregation::Max,
                    telemetry_path: "health.last_checkpoint_duration".into(),
                    telemetry_type: TelemetryType::Float,
                },
            },
            MetricOrder::Operator {
                name: "Source: Data Stream".into(),
                metric: MetricSpec {
                    metric: "records-lag-max".into(),
                    agg: Aggregation::Sum,
                    telemetry_path: "flow.source_records_lag_max".into(),
                    telemetry_type: TelemetryType::Integer,
                },
                position: PlanPositionSpec::Any,
            },
            MetricOrder::Operator {
                name: "Source: Supplement Stream".into(),
                metric: MetricSpec {
                    metric: "records-lag-max".into(),
                    agg: Aggregation::Sum,
                    telemetry_path: "supplement.source_records_lag_max".into(),
                    telemetry_type: TelemetryType::Integer,
                },
                position: PlanPositionSpec::Source,
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
        |        "telemetry_path": "flow.source_records_lag_max",
        |        "telemetry_type": Integer,
        |    }),
        |    Operator({
        |        "name": "Source: Supplement Stream",
        |        "position": source,
        |        "metric": "records-lag-max",
        |        "agg": sum,
        |        "telemetry_path": "supplement.source_records_lag_max",
        |        "telemetry_type": Integer,
        |    }),
        |]"##
            .trim_margin_with("|")
            .unwrap();

        assert_eq!(actual, expected);
    }
}
