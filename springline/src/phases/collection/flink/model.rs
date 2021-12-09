use std::collections::{HashMap, HashSet};
use std::fmt::{self, Display};
use std::hash::Hash;
use std::marker::PhantomData;
use std::time::Duration;

use once_cell::sync::Lazy;
use proctor::elements::{Telemetry, TelemetryValue, Timestamp};
use proctor::error::TelemetryError;
use serde::de::Error;
use serde::{de, Deserialize, Deserializer, Serialize};
use serde_with::{formats::Flexible, serde_as, DurationMilliSeconds};

use crate::settings::{Aggregation, MetricOrder};

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

#[tracing::instrument(level = "debug", skip(metrics, orders))]
pub fn build_telemetry<M>(metrics: M, orders: HashMap<String, Vec<MetricOrder>>) -> Result<Telemetry, TelemetryError>
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
            tracing::warn!(%id, %agg, "failed to correlate metric form to known Flink scopes - defaulting to camelCase");
            format!("{}", agg)
        },
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Job {
    pub id: String,
    pub status: JobState,
}

#[derive(Debug, Display, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
enum JobState {
    Initializing,
    Created,
    Running,
    Failing,
    Failed,
    Cancelling,
    Canceled,
    Finished,
    Restarting,
    Suspended,
    Reconciling,
}

#[derive(Debug, Display, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
enum TaskState {
    Scheduled,
    Created,
    Running,
    Failed,
    Canceling,
    Canceled,
    Finished,
    Deploying,
    Reconciling,
    Initializing,
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct JobDetail {
    pub jid: String,
    pub name: String,
    #[serde(alias = "isStoppable")]
    pub is_stoppable: bool,
    pub state: JobState,
    #[serde(alias = "start-time", deserialize_with = "deserialize_timestamp_millis")]
    pub start_time: Timestamp,
    #[serde(alias = "end-time", deserialize_with = "deserialize_opt_timestamp_millis")]
    pub end_time: Option<Timestamp>,
    #[serde_as(as = "DurationMilliSeconds<String, Flexible>")]
    pub duration: Duration,
    #[serde(alias = "maxParallelism", deserialize_with = "deserialize_i64_as_opt_usize")]
    pub max_parallelism: Option<usize>,
    #[serde(deserialize_with = "deserialize_timestamp_millis")]
    pub now: Timestamp,
    #[serde(default, deserialize_with = "deserialize_key_timestamps")]
    pub timestamps: HashMap<JobState, Timestamp>,
    #[serde(default)]
    pub vertices: Vec<VertexDetail>,
    #[serde(default, alias = "status-counts")]
    pub status_counts: HashMap<TaskState, usize>,
}


#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct VertexDetail {
    pub id: String,
    pub name: String,
    #[serde(alias = "maxParallelism", deserialize_with = "deserialize_i64_as_opt_usize")]
    pub max_parallelism: Option<usize>,
    pub parallelism: usize,
    pub status: JobState,
    #[serde(alias = "start-time", deserialize_with = "deserialize_timestamp_millis")]
    pub start_time: Timestamp,
    #[serde(alias = "end-time", deserialize_with = "deserialize_opt_timestamp_millis")]
    pub end_time: Option<Timestamp>,
    #[serde_as(as = "DurationMilliSeconds<String, Flexible>")]
    pub duration: Duration,
    pub tasks: HashMap<TaskState, usize>,
    pub metrics: HashMap<String, TelemetryValue>,
}

fn deserialize_timestamp_millis<'de, D>(deserializer: D) -> Result<Timestamp, D::Error>
where
    D: Deserializer<'de>,
{
    let millis = i64::deserialize(deserializer)?;
    Ok(Timestamp::from_milliseconds(millis))
}

fn deserialize_opt_timestamp_millis<'de, D>(deserializer: D) -> Result<Option<Timestamp>, D::Error>
where
    D: Deserializer<'de>,
{
    let millis = i64::deserialize(deserializer)?;
    let result = if millis < 0 { None } else { Some(Timestamp::from_milliseconds(millis)) };
    Ok(result)
}

#[tracing::instrument(level = "debug", skip(deserializer))]
fn deserialize_i64_as_opt_usize<'de, D>(deserializer: D) -> Result<Option<usize>, D::Error>
where
    D: Deserializer<'de>,
{
    let val = i64::deserialize(deserializer)?;
    let result = if val < 0 { None } else { Some(val as usize) };
    Ok(result)
}

fn deserialize_key_timestamps<'de, D, S>(deserializer: D) -> Result<HashMap<S, Timestamp>, D::Error>
where
    D: Deserializer<'de>,
    S: Eq + Hash + Deserialize<'de>,
{
    struct KeyTimestampsVisitor<S0> {
        marker: PhantomData<fn() -> HashMap<S0, Timestamp>>,
    }

    impl<S0> KeyTimestampsVisitor<S0> {
        fn new() -> Self {
            Self { marker: PhantomData }
        }
    }

    impl<'de0, S0> de::Visitor<'de0> for KeyTimestampsVisitor<S0>
    where
        S0: Eq + Hash + Deserialize<'de0>,
    {
        type Value = HashMap<S0, Timestamp>;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "map of key timestamps")
        }

        fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
        where
            M: de::MapAccess<'de0>,
        {
            let mut map = HashMap::with_capacity(access.size_hint().unwrap_or(0));
            while let Some((key, ts)) = access.next_entry()? {
                let timestamp = Timestamp::from_milliseconds(ts);
                map.insert(key, timestamp);
            }

            Ok(map)
        }
    }

    deserializer.deserialize_map(KeyTimestampsVisitor::new())
}


#[cfg(test)]
mod tests {
    use claim::*;
    use pretty_assertions::assert_eq;
    use serde_json::json;
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

    #[test]
    fn test_job_vertices_parsing() {
        let jobs_json = r##"|{
        |    "jobs": [
        |        { "id": "0771e8332dc401d254a140a707169a48", "status": "RUNNING" },
        |        { "id": "08734e8332dc401d254a140a707169c98", "status": "CANCELLING" }
        |    ]
        |}"##
            .trim_margin_with("|")
            .unwrap();

        let my_jobs: serde_json::Value = assert_ok!(serde_json::from_str(&jobs_json));
        let jobs: Vec<Job> = match my_jobs["jobs"].as_array() {
            Some(js) => js.iter().map(|j| assert_ok!(serde_json::from_value(j.clone()))).collect(),
            None => Vec::default(),
        };

        assert_eq!(
            jobs,
            vec![
                Job {
                    id: "0771e8332dc401d254a140a707169a48".to_string(),
                    status: JobState::Running,
                },
                Job {
                    id: "08734e8332dc401d254a140a707169c98".to_string(),
                    status: JobState::Cancelling,
                },
            ]
        );
    }

    #[test]
    fn test_vertex_detail_deser() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_vertex_detail_deser");
        let _ = main_span.enter();

        let json_rep = r##"|{
        |    "id": "cbc357ccb763df2852fee8c4fc7d55f2",
        |    "name": "Source: Custom Source -> Timestamps/Watermarks",
        |    "maxParallelism": 128,
        |    "parallelism": 1,
        |    "status": "RUNNING",
        |    "start-time": 1638989054310,
        |    "end-time": -1,
        |    "duration": 35010565,
        |    "tasks": {
        |        "SCHEDULED": 0,
        |        "FINISHED": 0,
        |        "FAILED": 0,
        |        "CANCELING": 0,
        |        "CANCELED": 0,
        |        "DEPLOYING": 0,
        |        "RECONCILING": 0,
        |        "CREATED": 0,
        |        "RUNNING": 1,
        |        "INITIALIZING": 0
        |    },
        |    "metrics": {
        |        "read-bytes": 0,
        |        "read-bytes-complete": true,
        |        "write-bytes": 32768,
        |        "write-bytes-complete": true,
        |        "read-records": 0,
        |        "read-records-complete": true,
        |        "write-records": 1012,
        |        "write-records-complete": true
        |    }
        |}"##
            .trim_margin_with("|")
            .unwrap();

        let actual: VertexDetail = assert_ok!(serde_json::from_str(&json_rep));
        assert_eq!(
            actual,
            VertexDetail {
                id: "cbc357ccb763df2852fee8c4fc7d55f2".to_string(),
                name: "Source: Custom Source -> Timestamps/Watermarks".to_string(),
                max_parallelism: Some(128),
                parallelism: 1,
                status: JobState::Running,
                start_time: Timestamp::new(1638989054, 310 * 1_000_000),
                end_time: None,
                duration: Duration::from_millis(35010565),
                tasks: maplit::hashmap! {
                    TaskState::Scheduled => 0,
                    TaskState::Finished => 0,
                    TaskState::Failed => 0,
                    TaskState::Canceling => 0,
                    TaskState::Canceled => 0,
                    TaskState::Deploying => 0,
                    TaskState::Reconciling => 0,
                    TaskState::Created => 0,
                    TaskState::Running => 1,
                    TaskState::Initializing => 0,
                },
                metrics: maplit::hashmap! {
                    "read-bytes".to_string() => 0_i64.into(),
                    "read-bytes-complete".to_string() => true.into(),
                    "write-bytes".to_string() => 32768_i64.into(),
                    "write-bytes-complete".to_string() => true.into(),
                    "read-records".to_string() => 0_i64.into(),
                    "read-records-complete".to_string() => true.into(),
                    "write-records".to_string() => 1012_i64.into(),
                    "write-records-complete".to_string() => true.into(),
                },
            }
        )
    }

    #[test]
    fn test_job_detail_deser() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_job_detail_deser");
        let _ = main_span.enter();

        let json_rep = r##"|{
        |   "jid": "0771e8332dc401d254a140a707169a48",
        |    "name": "CarTopSpeedWindowingExample",
        |    "isStoppable": false,
        |    "state": "RUNNING",
        |    "start-time": 1638989050332,
        |    "end-time": -1,
        |    "duration": 35014543,
        |    "maxParallelism": -1,
        |    "now": 1639024064875,
        |    "timestamps": {
        |        "CREATED": 1638989050521,
        |        "FAILED": 0,
        |        "RUNNING": 1638989054173,
        |        "CANCELED": 0,
        |        "CANCELLING": 0,
        |        "SUSPENDED": 0,
        |        "FAILING": 0,
        |        "RESTARTING": 0,
        |        "FINISHED": 0,
        |        "INITIALIZING": 1638989050332,
        |        "RECONCILING": 0
        |    },
        |    "vertices": [
        |        {
        |            "id": "cbc357ccb763df2852fee8c4fc7d55f2",
        |            "name": "Source: Custom Source -> Timestamps/Watermarks",
        |            "maxParallelism": 128,
        |            "parallelism": 1,
        |            "status": "RUNNING",
        |            "start-time": 1638989054310,
        |            "end-time": -1,
        |            "duration": 35010565,
        |            "tasks": {
        |                "SCHEDULED": 0,
        |                "FINISHED": 0,
        |                "FAILED": 0,
        |                "CANCELING": 0,
        |                "CANCELED": 0,
        |                "DEPLOYING": 0,
        |                "RECONCILING": 0,
        |                "CREATED": 0,
        |                "RUNNING": 1,
        |                "INITIALIZING": 0
        |            },
        |            "metrics": {
        |                "read-bytes": 0,
        |                "read-bytes-complete": true,
        |                "write-bytes": 32768,
        |                "write-bytes-complete": true,
        |                "read-records": 0,
        |                "read-records-complete": true,
        |                "write-records": 1012,
        |                "write-records-complete": true
        |            }
        |        },
        |        {
        |            "id": "90bea66de1c231edf33913ecd54406c1",
        |            "name": "Window(GlobalWindows(), DeltaTrigger, TimeEvictor, ComparableAggregator, PassThroughWindowFunction) -> Sink: Print to Std. Out",
        |            "maxParallelism": 128,
        |            "parallelism": 1,
        |            "status": "RUNNING",
        |            "start-time": 1638989054320,
        |            "end-time": -1,
        |            "duration": 35010555,
        |            "tasks": {
        |                "SCHEDULED": 0,
        |                "FINISHED": 0,
        |                "FAILED": 0,
        |                "CANCELING": 0,
        |                "CANCELED": 0,
        |                "DEPLOYING": 0,
        |                "RECONCILING": 0,
        |                "CREATED": 0,
        |                "RUNNING": 1,
        |                "INITIALIZING": 0
        |            },
        |            "metrics": {
        |                "read-bytes": 40637,
        |                "read-bytes-complete": true,
        |                "write-bytes": 0,
        |                "write-bytes-complete": true,
        |                "read-records": 1010,
        |                "read-records-complete": true,
        |                "write-records": 0,
        |                "write-records-complete": true
        |            }
        |        }
        |    ],
        |    "status-counts": {
        |        "SCHEDULED": 0,
        |        "FINISHED": 0,
        |        "FAILED": 0,
        |        "CANCELING": 0,
        |        "CANCELED": 0,
        |        "DEPLOYING": 0,
        |        "RECONCILING": 0,
        |        "CREATED": 0,
        |        "RUNNING": 2,
        |        "INITIALIZING": 0
        |    },
        |    "plan": {
        |        "jid": "0771e8332dc401d254a140a707169a48",
        |        "name": "CarTopSpeedWindowingExample",
        |        "nodes": [
        |            {
        |                "id": "90bea66de1c231edf33913ecd54406c1",
        |                "parallelism": 1,
        |                "operator": "",
        |                "operator_strategy": "",
        |                "description": "Window(GlobalWindows(), DeltaTrigger, TimeEvictor, ComparableAggregator, PassThroughWindowFunction) -&gt; Sink: Print to Std. Out",
        |                "inputs": [
        |                    {
        |                        "num": 0,
        |                        "id": "cbc357ccb763df2852fee8c4fc7d55f2",
        |                        "ship_strategy": "HASH",
        |                        "exchange": "pipelined_bounded"
        |                    }
        |                ],
        |                "optimizer_properties": {}
        |            },
        |            {
        |                "id": "cbc357ccb763df2852fee8c4fc7d55f2",
        |                "parallelism": 1,
        |                "operator": "",
        |                "operator_strategy": "",
        |                "description": "Source: Custom Source -&gt; Timestamps/Watermarks",
        |                "optimizer_properties": {}
        |            }
        |        ]
        |    }
        |}"##.trim_margin_with("|").unwrap();

        let actual: JobDetail = assert_ok!(serde_json::from_str(&json_rep));
        assert_eq!(
            actual,
            JobDetail {
                jid: "0771e8332dc401d254a140a707169a48".to_string(),
                name: "CarTopSpeedWindowingExample".to_string(),
                is_stoppable: false,
                state: JobState::Running,
                start_time: Timestamp::new(1638989050, 332_000_000),
                end_time: None,
                duration: Duration::from_millis(35014543),
                max_parallelism: None,
                now: Timestamp::new(1639024064, 875_000_000),
                timestamps: maplit::hashmap! {
                    JobState::Created => Timestamp::new(1638989050, 521_000_000),
                    JobState::Failed => Timestamp::ZERO,
                    JobState::Running => Timestamp::new(1638989054, 173_000_000),
                    JobState::Canceled => Timestamp::ZERO,
                    JobState::Cancelling => Timestamp::ZERO,
                    JobState::Suspended => Timestamp::ZERO,
                    JobState::Failing => Timestamp::ZERO,
                    JobState::Restarting => Timestamp::ZERO,
                    JobState::Finished => Timestamp::ZERO,
                    JobState::Initializing => Timestamp::new(1638989050, 332_000_000),
                    JobState::Reconciling => Timestamp::ZERO,
                },
                vertices: vec![
                    VertexDetail {
                        id: "cbc357ccb763df2852fee8c4fc7d55f2".to_string(),
                        name: "Source: Custom Source -> Timestamps/Watermarks".to_string(),
                        max_parallelism: Some(128),
                        parallelism: 1,
                        status: JobState::Running,
                        start_time: Timestamp::new(1638989054, 310 * 1_000_000),
                        end_time: None,
                        duration: Duration::from_millis(35010565),
                        tasks: maplit::hashmap! {
                            TaskState::Scheduled => 0,
                            TaskState::Finished => 0,
                            TaskState::Failed => 0,
                            TaskState::Canceling => 0,
                            TaskState::Canceled => 0,
                            TaskState::Deploying => 0,
                            TaskState::Reconciling => 0,
                            TaskState::Created => 0,
                            TaskState::Running => 1,
                            TaskState::Initializing => 0,
                        },
                        metrics: maplit::hashmap! {
                            "read-bytes".to_string() => 0_i64.into(),
                            "read-bytes-complete".to_string() => true.into(),
                            "write-bytes".to_string() => 32768_i64.into(),
                            "write-bytes-complete".to_string() => true.into(),
                            "read-records".to_string() => 0_i64.into(),
                            "read-records-complete".to_string() => true.into(),
                            "write-records".to_string() => 1012_i64.into(),
                            "write-records-complete".to_string() => true.into(),
                        },
                    },
                    VertexDetail {
                        id: "90bea66de1c231edf33913ecd54406c1".to_string(),
                        name: "Window(GlobalWindows(), DeltaTrigger, TimeEvictor, ComparableAggregator, \
                               PassThroughWindowFunction) -> Sink: Print to Std. Out"
                            .to_string(),
                        max_parallelism: Some(128),
                        parallelism: 1,
                        status: JobState::Running,
                        start_time: Timestamp::new(1638989054, 320 * 1_000_000),
                        end_time: None,
                        duration: Duration::from_millis(35010555),
                        tasks: maplit::hashmap! {
                            TaskState::Scheduled => 0,
                            TaskState::Finished => 0,
                            TaskState::Failed => 0,
                            TaskState::Canceling => 0,
                            TaskState::Canceled => 0,
                            TaskState::Deploying => 0,
                            TaskState::Reconciling => 0,
                            TaskState::Created => 0,
                            TaskState::Running => 1,
                            TaskState::Initializing => 0,
                        },
                        metrics: maplit::hashmap! {
                            "read-bytes".to_string() => 40637_i64.into(),
                            "read-bytes-complete".to_string() => true.into(),
                            "write-bytes".to_string() => 0_i64.into(),
                            "write-bytes-complete".to_string() => true.into(),
                            "read-records".to_string() => 1010_i64.into(),
                            "read-records-complete".to_string() => true.into(),
                            "write-records".to_string() => 0_i64.into(),
                            "write-records-complete".to_string() => true.into(),
                        },
                    },
                ],
                status_counts: maplit::hashmap! {
                    TaskState::Scheduled => 0,
                    TaskState::Finished => 0,
                    TaskState::Failed => 0,
                    TaskState::Canceling => 0,
                    TaskState::Canceled => 0,
                    TaskState::Deploying => 0,
                    TaskState::Reconciling => 0,
                    TaskState::Created => 0,
                    TaskState::Running => 2,
                    TaskState::Initializing => 0,
                },
            }
        )
    }
}
