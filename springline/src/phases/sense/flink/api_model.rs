use std::collections::{HashMap, HashSet};

use once_cell::sync::Lazy;
use proctor::elements::{Telemetry, TelemetryValue};
use proctor::error::TelemetryError;
use serde::{Deserialize, Serialize};

use super::{Aggregation, MetricOrder};

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlinkMetricResponse(pub Vec<FlinkMetric>);

impl IntoIterator for FlinkMetricResponse {
    type IntoIter = std::vec::IntoIter<Self::Item>;
    type Item = FlinkMetric;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlinkMetric {
    pub id: String,
    #[serde(flatten)]
    pub values: HashMap<Aggregation, TelemetryValue>,
}

impl FlinkMetric {
    #[allow(dead_code)]
    fn populate_telemetry<'m, O>(&self, telemetry: &mut Telemetry, orders: O)
    where
        O: IntoIterator<Item = &'m MetricOrder>,
    {
        for o in orders.into_iter() {
            let agg = o.agg;
            match self.values.get(&agg) {
                None => tracing::warn!(metric=%o.metric, %agg, "metric order not found in flink response."),
                Some(metric_value) => match metric_value.clone().try_cast(o.telemetry_type) {
                    Err(err) => tracing::error!(
                        error=?err, metric=%o.metric, ?metric_value, order=?o,
                        "Unable to read ordered type in flink metric response - skipping."
                    ),
                    Ok(value) => {
                        let _ = telemetry.insert(o.telemetry_path.clone(), value);
                    },
                },
            }
        }
    }
}

#[tracing::instrument(level = "trace", skip(metrics, orders))]
pub fn build_telemetry<M>(metrics: M, orders: &HashMap<String, Vec<MetricOrder>>) -> Result<Telemetry, TelemetryError>
where
    M: IntoIterator<Item = FlinkMetric>,
{
    let mut telemetry = Telemetry::default();

    let mut satisfied = HashSet::new();

    for m in metrics.into_iter() {
        match orders.get(m.id.as_str()) {
            Some(os) => {
                satisfied.insert(m.id.clone());
                m.populate_telemetry(&mut telemetry, os);
            },
            None => {
                tracing::warn!(unexpected_metric=?m, "unexpected metric in response not ordered - adding with minimal translation");
                m.values.into_iter().for_each(|(agg, val)| {
                    let key = format!("{}{}", m.id, suffix_for(m.id.as_str(), agg));
                    let _ = telemetry.insert(key, val);
                });
            },
        }
    }

    let all: HashSet<String> = orders.keys().cloned().collect();
    let unfulfilled = all.difference(&satisfied).collect::<HashSet<_>>();
    if !unfulfilled.is_empty() {
        tracing::warn!(?unfulfilled, "some metrics orders were not fulfilled.");
    }

    Ok(telemetry)
}

/// sigh -- each flink scope follows it's own metric format convention. This function attempts to
/// fashion a corresponding aggregation suffix.
#[allow(dead_code)]
fn suffix_for(id: &str, agg: Aggregation) -> String {
    let forms: Lazy<regex::RegexSet> = Lazy::new(|| {
        regex::RegexSet::new(&[
            r##"^[a-z]+[a-zA-Z]+$"##,                     // camelCase: Jobs, Kinesis
            r##"^[a-z]+[a-zA-Z]*(\.[a-z]+[a-zA-Z]*)+$"##, // .camelCase: Task vertex
            r##"^[a-z]+[-a-z]+$"##,                       // kabab-case: Kafka
            r##"^[A-Z]+[a-zA-Z]*(\.[A-Z]+[a-zA-Z]*)*$"##, // .PascalCase: TaskManagers
        ])
        .unwrap()
    });

    match forms.matches(id).into_iter().take(1).next() {
        Some(0) => format!("{}", agg),                             // camelCase - Jobs and Kinesis
        Some(1) => format!(".{}", agg.to_string().to_lowercase()), // .camelCase - Task vertex
        Some(2) => format!("-{}", agg.to_string().to_lowercase()), // kabab-case - Kafka
        Some(3) => format!(".{}", agg),                            // .PascalCase - TaskManagers
        _ => {
            tracing::warn!(%id, %agg, "failed to match metric id to known Flink forms - defaulting to camelCase");
            format!("{}", agg)
        },
    }
}

#[cfg(test)]
mod tests {
    use claim::*;
    use pretty_assertions::assert_eq;
    use trim_margin::MarginTrimmable;

    use super::*;

    #[test]
    fn test_suffix_for() {
        use self::Aggregation::*;

        assert_eq!(&suffix_for("", Min), "Min");
        assert_eq!(&suffix_for("*&^@(*#(*", Value), "Value");
        assert_eq!(&suffix_for("uptime", Max), "Max");
        assert_eq!(&suffix_for("numRestarts", Max), "Max");
        assert_eq!(&suffix_for("numberOfCompletedCheckpoints", Max), "Max");
        assert_eq!(&suffix_for("Status.JVM.CPU.Load", Max), ".Max");
        assert_eq!(&suffix_for("buffers.inputQueueLength", Max), ".max");
        assert_eq!(&suffix_for("records-lag-max", Value), "-value");
    }

    #[test]
    fn test_flink_metric_response_json_deser() {
        let json = r##"|[
                |  {
                |    "id": "metric1",
                |    "min": 1,
                |    "max": 34,
                |    "avg": 15,
                |    "sum": 45
                |  },
                |  {
                |    "id": "metric2",
                |    "min": 2,
                |    "max": 14,
                |    "avg": 7,
                |    "sum": 16
                |  }
                |]"##
            .trim_margin_with("|")
            .unwrap();

        let actual: FlinkMetricResponse = assert_ok!(serde_json::from_str(&json));
        assert_eq!(
            actual,
            FlinkMetricResponse(vec![
                FlinkMetric {
                    id: "metric1".to_string(),
                    values: maplit::hashmap! {
                        Aggregation::Min => 1_i64.into(),
                        Aggregation::Max => 34_i64.into(),
                        Aggregation::Avg => 15_i64.into(),
                        Aggregation::Sum => 45_i64.into(),
                    },
                },
                FlinkMetric {
                    id: "metric2".to_string(),
                    values: maplit::hashmap! {
                        Aggregation::Min => 2_i64.into(),
                        Aggregation::Max => 14_i64.into(),
                        Aggregation::Avg => 7_i64.into(),
                        Aggregation::Sum => 16_i64.into(),
                    },
                },
            ])
        );
    }
}
