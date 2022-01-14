use std::collections::HashMap;
use std::fmt::{self, Display};

use itertools::Itertools;
use proctor::elements::telemetry::combine::{self, TelemetryCombinator};
use proctor::elements::TelemetryType;
use serde::de::{self, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::ser::SerializeTupleStruct;
use serde::{Deserialize, Serialize, Serializer};

#[derive(Debug, Clone, PartialEq)]
pub struct MetricOrder {
    pub scope: FlinkScope,
    pub metric: String,
    pub agg: Aggregation,
    pub telemetry_path: String,
    pub telemetry_type: TelemetryType,
}

impl MetricOrder {
    pub fn organize_by_scope(orders: &[MetricOrder]) -> HashMap<FlinkScope, Vec<MetricOrder>> {
        let mut result: HashMap<FlinkScope, Vec<MetricOrder>> = HashMap::default();

        for (scope, group) in &orders.iter().group_by(|o| o.scope) {
            let orders = result.entry(scope).or_insert_with(Vec::new);
            let group: Vec<MetricOrder> = group.cloned().collect();
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
        let mut state = serializer.serialize_tuple_struct("MetricOrder", 5)?;
        state.serialize_field(&self.scope)?;
        state.serialize_field(&self.metric)?;
        state.serialize_field(&self.agg)?;
        state.serialize_field(&self.telemetry_path)?;
        state.serialize_field(&self.telemetry_type)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for MetricOrder {
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
            type Value = MetricOrder;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("struct MetricOrder")
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

                Ok(MetricOrder { scope, metric, agg, telemetry_path, telemetry_type })
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

                Ok(MetricOrder { scope, metric, agg, telemetry_path, telemetry_type })
            }
        }

        // const FIELDS: &'static [&'static str] = &["scope", "metric", "agg", "telemetry_path",
        // "telemetry_type"];
        deserializer.deserialize_tuple_struct("MetricOrder", 5, OrderVisitor)
    }
}

#[derive(Debug, Display, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FlinkScope {
    Jobs,
    Task,
    TaskManagers,
    Kafka,
    Kinesis,
    Other,
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
