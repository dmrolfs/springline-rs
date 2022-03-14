use super::FlinkError;
use either::Either;
use std::collections::HashMap;
use std::fmt::{self, Display};
use std::hash::Hash;
use std::marker::PhantomData;
use std::time::Duration;

use proctor::elements::telemetry::{TableType, TableValue};
use proctor::elements::{TelemetryType, TelemetryValue, Timestamp};
use serde::{de, Deserialize, Deserializer, Serialize};
use serde_with::serde_as;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobSummary {
    pub id: JobId,
    pub status: JobState,
}

pub const JOB_STATES: [JobState; 11] = [
    JobState::Initializing,
    JobState::Created,
    JobState::Running,
    JobState::Failing,
    JobState::Failed,
    JobState::Cancelling,
    JobState::Canceled,
    JobState::Finished,
    JobState::Restarting,
    JobState::Suspended,
    JobState::Reconciling,
];

#[derive(Debug, Display, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum JobState {
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

impl JobState {
    #[allow(dead_code)]
    pub const fn is_active(&self) -> bool {
        !matches!(self, Self::Finished | Self::Failed | Self::Canceled | Self::Suspended)
    }

    pub const fn is_engaged(&self) -> bool {
        matches!(self, Self::Running | Self::Finished | Self::Reconciling)
        // !matches!(
        //     self,
        //     Self::Initializing | Self::Failing | Self::Failed | Self::Cancelling | Self::Canceled | Self:: Suspended
        // )
    }
}

pub const TASK_STATES: [TaskState; 10] = [
    TaskState::Scheduled,
    TaskState::Created,
    TaskState::Running,
    TaskState::Failed,
    TaskState::Canceling,
    TaskState::Canceled,
    TaskState::Finished,
    TaskState::Deploying,
    TaskState::Reconciling,
    TaskState::Initializing,
];

#[derive(Debug, Display, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum TaskState {
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

impl TaskState {
    #[allow(dead_code)]
    pub const fn is_active(&self) -> bool {
        !matches!(self, Self::Finished | Self::Failed | Self::Canceled)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JarSummary {
    pub id: JarId,

    pub name: String,

    #[serde(
        rename = "uploaded",
        serialize_with = "Timestamp::serialize_as_secs_i64",
        deserialize_with = "Timestamp::deserialize_secs_i64"
    )]
    pub uploaded_at: Timestamp,

    pub entry: Vec<JarEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JarEntry {
    pub name: String,
    pub description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JarId(String);

impl JarId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

impl fmt::Display for JarId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for JarId {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl From<String> for JarId {
    fn from(rep: String) -> Self {
        Self(rep)
    }
}

impl From<JarId> for String {
    fn from(jid: JarId) -> Self {
        jid.0
    }
}

impl From<&str> for JarId {
    fn from(rep: &str) -> Self {
        Self(rep.to_string())
    }
}

impl From<JarId> for TelemetryValue {
    fn from(jar_id: JarId) -> Self {
        Self::Text(jar_id.0)
    }
}

impl TryFrom<TelemetryValue> for JarId {
    type Error = FlinkError;

    fn try_from(value: TelemetryValue) -> Result<Self, Self::Error> {
        match value {
            TelemetryValue::Text(s) => Ok(Self(s)),
            val => Err(FlinkError::ExpectedTelemetryType(TelemetryType::Text, val)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JobId(String);

impl JobId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for JobId {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl From<String> for JobId {
    fn from(rep: String) -> Self {
        Self(rep)
    }
}

impl From<JobId> for String {
    fn from(jid: JobId) -> Self {
        jid.0
    }
}

impl From<&str> for JobId {
    fn from(rep: &str) -> Self {
        Self(rep.to_string())
    }
}

impl From<JobId> for TelemetryValue {
    fn from(job_id: JobId) -> Self {
        Self::Text(job_id.0)
    }
}

impl TryFrom<TelemetryValue> for JobId {
    type Error = FlinkError;

    fn try_from(value: TelemetryValue) -> Result<Self, Self::Error> {
        match value {
            TelemetryValue::Text(s) => Ok(Self(s)),
            val => Err(FlinkError::ExpectedTelemetryType(TelemetryType::Text, val)),
        }
    }
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobDetail {
    pub jid: JobId,
    pub name: String,
    #[serde(alias = "isStoppable")]
    pub is_stoppable: bool,
    pub state: JobState,
    #[serde(alias = "start-time", deserialize_with = "deserialize_timestamp_millis")]
    pub start_time: Timestamp,
    #[serde(alias = "end-time", deserialize_with = "deserialize_opt_timestamp_millis")]
    pub end_time: Option<Timestamp>,
    #[serde(deserialize_with = "deserialize_opt_duration_millis")]
    pub duration: Option<Duration>,
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VertexId(String);

impl VertexId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

impl fmt::Display for VertexId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for VertexId {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl From<String> for VertexId {
    fn from(rep: String) -> Self {
        Self(rep)
    }
}

impl From<VertexId> for String {
    fn from(vid: VertexId) -> Self {
        vid.0
    }
}

impl From<&str> for VertexId {
    fn from(rep: &str) -> Self {
        Self(rep.to_string())
    }
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VertexDetail {
    pub id: VertexId,
    pub name: String,
    #[serde(alias = "maxParallelism", deserialize_with = "deserialize_i64_as_opt_usize")]
    pub max_parallelism: Option<usize>,
    pub parallelism: usize,
    pub status: TaskState,
    #[serde(alias = "start-time", deserialize_with = "deserialize_timestamp_millis")]
    pub start_time: Timestamp,
    #[serde(alias = "end-time", deserialize_with = "deserialize_opt_timestamp_millis")]
    pub end_time: Option<Timestamp>,
    #[serde(deserialize_with = "deserialize_opt_duration_millis")]
    pub duration: Option<Duration>,
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

fn deserialize_opt_duration_millis<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    let millis = i64::deserialize(deserializer)?;
    let duration = if millis < 0 { None } else { Some(Duration::from_millis(millis as u64)) };
    Ok(duration)
}

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

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobSavepointReport {
    pub completed: HashMap<JobId, SavepointLocation>,
    pub failed: HashMap<JobId, FailureReason>,
}

impl JobSavepointReport {
    pub fn new(
        completed_statuses: Vec<(JobId, SavepointStatus)>, failed_statuses: Vec<(JobId, SavepointStatus)>,
    ) -> Self {
        let completed = completed_statuses
            .into_iter()
            .map(|(job, status)| {
                let location = status
                    .operation
                    .expect("savepoint status must have an operation")
                    .left()
                    .expect("successful savepoint location is not populated.");
                (job, location)
            })
            .collect();

        let failed = failed_statuses
            .into_iter()
            .map(|(job, status)| {
                let failure = status
                    .operation
                    .expect("failed savepoint status must have an operation")
                    .right()
                    .expect("failed savepoint failure reason is not populated.");
                (job, failure)
            })
            .collect();

        Self { completed, failed }
    }
}

impl From<JobSavepointReport> for TelemetryValue {
    fn from(report: JobSavepointReport) -> Self {
        let mut telemetry = TableValue::new();

        let completed: TableType = report
            .completed
            .into_iter()
            .map(|(job, location)| (job.into(), location.into()))
            .collect();
        telemetry.insert("completed".to_string(), completed.into());

        let failed: TableType = report
            .failed
            .into_iter()
            .map(|(job, failure)| (job.into(), failure.into()))
            .collect();
        telemetry.insert("failed".to_string(), failed.into());

        Self::Table(telemetry)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SavepointStatus {
    pub status: OperationStatus,
    pub operation: Option<Either<SavepointLocation, FailureReason>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationStatus {
    InProgress,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SavepointLocation(String);

impl SavepointLocation {
    pub fn new(location: impl Into<String>) -> Self {
        Self(location.into())
    }
}

impl fmt::Display for SavepointLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for SavepointLocation {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<SavepointLocation> for TelemetryValue {
    fn from(location: SavepointLocation) -> Self {
        Self::Text(location.0)
    }
}

impl AsRef<str> for SavepointLocation {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailureReason(String);

impl FailureReason {
    pub fn new(reason: impl Into<String>) -> Self {
        Self(reason.into())
    }
}

impl fmt::Display for FailureReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for FailureReason {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<FailureReason> for TelemetryValue {
    fn from(reason: FailureReason) -> Self {
        Self::Text(reason.0)
    }
}

impl AsRef<str> for FailureReason {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

#[cfg(test)]
mod tests {
    use claim::*;
    use pretty_assertions::assert_eq;
    use trim_margin::MarginTrimmable;

    use super::*;

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
        let jobs: Vec<JobSummary> = match my_jobs["jobs"].as_array() {
            Some(js) => js.iter().map(|j| assert_ok!(serde_json::from_value(j.clone()))).collect(),
            None => Vec::default(),
        };

        assert_eq!(
            jobs,
            vec![
                JobSummary {
                    id: JobId::new("0771e8332dc401d254a140a707169a48"),
                    status: JobState::Running,
                },
                JobSummary {
                    id: JobId::new("08734e8332dc401d254a140a707169c98"),
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
                id: VertexId("cbc357ccb763df2852fee8c4fc7d55f2".to_string()),
                name: "Source: Custom Source -> Timestamps/Watermarks".to_string(),
                max_parallelism: Some(128),
                parallelism: 1,
                status: TaskState::Running,
                start_time: Timestamp::new(1638989054, 310 * 1_000_000),
                end_time: None,
                duration: Some(Duration::from_millis(35010565)),
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
                jid: JobId("0771e8332dc401d254a140a707169a48".to_string()),
                name: "CarTopSpeedWindowingExample".to_string(),
                is_stoppable: false,
                state: JobState::Running,
                start_time: Timestamp::new(1638989050, 332_000_000),
                end_time: None,
                duration: Some(Duration::from_millis(35014543)),
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
                        id: VertexId("cbc357ccb763df2852fee8c4fc7d55f2".to_string()),
                        name: "Source: Custom Source -> Timestamps/Watermarks".to_string(),
                        max_parallelism: Some(128),
                        parallelism: 1,
                        status: TaskState::Running,
                        start_time: Timestamp::new(1638989054, 310 * 1_000_000),
                        end_time: None,
                        duration: Some(Duration::from_millis(35010565)),
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
                        id: VertexId("90bea66de1c231edf33913ecd54406c1".to_string()),
                        name: "Window(GlobalWindows(), DeltaTrigger, TimeEvictor, ComparableAggregator, \
                               PassThroughWindowFunction) -> Sink: Print to Std. Out"
                            .to_string(),
                        max_parallelism: Some(128),
                        parallelism: 1,
                        status: TaskState::Running,
                        start_time: Timestamp::new(1638989054, 320 * 1_000_000),
                        end_time: None,
                        duration: Some(Duration::from_millis(35010555)),
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
