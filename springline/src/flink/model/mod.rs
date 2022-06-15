#[cfg(test)]
mod tests;

pub mod catalog;
pub mod portfolio;

use std::collections::HashMap;
use std::fmt::{self, Debug, Display};
use std::hash::Hash;
use std::marker::PhantomData;
use std::time::Duration;

use either::Either;
use pretty_snowflake::Id;
use proctor::elements::telemetry::{TableType, TableValue};
use proctor::elements::{TelemetryType, TelemetryValue, Timestamp};
use proctor::ProctorIdGenerator;
use serde::{de, Deserialize, Deserializer, Serialize};
use serde_with::serde_as;

use super::FlinkError;
use super::MetricCatalog;

pub type CorrelationId = Id<MetricCatalog>;
pub type CorrelationGenerator = ProctorIdGenerator<MetricCatalog>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    }

    pub const fn is_stopped(&self) -> bool {
        matches!(self, Self::Failed | Self::Canceled | Self::Finished | Self::Suspended)
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
#[derive(Clone, PartialEq, Serialize, Deserialize)]
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

impl Debug for JobDetail {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JobDetail")
            .field("job_id", &self.jid)
            .field("name", &self.name)
            .field("is_stoppable", &self.is_stoppable)
            .field("state", &self.state)
            .field(
                "[start - end]",
                &format!(
                    "[{} - {}]",
                    self.start_time,
                    self.end_time.map(|end| end.to_string()).unwrap_or_default()
                ),
            )
            .field("duration", &self.duration)
            .field("max_parallelism", &self.max_parallelism)
            .field("now", &self.now.to_string())
            .field(
                "timestamps",
                &self
                    .timestamps
                    .iter()
                    .map(|(s, ts)| (s, ts.to_string()))
                    .collect::<HashMap<_, _>>(),
            )
            .field("vertices", &self.vertices)
            .field("status_counts", &self.status_counts)
            .finish()
    }
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
    pub failed: HashMap<JobId, FailureCause>,
}

impl JobSavepointReport {
    pub fn new(
        completed_statuses: Vec<(JobId, SavepointStatus)>, failed_statuses: Vec<(JobId, SavepointStatus)>,
    ) -> Result<Self, FlinkError> {
        let completed = completed_statuses
            .into_iter()
            .flat_map(|(job_id, status)| {
                let location = status
                    .operation
                    .ok_or_else(|| FlinkError::UnexpectedValue {
                        expected: "completed savepoint status must have an operation".to_string(),
                        given: "<none>".to_string(),
                    })?
                    .either(Ok, |cause| Err(FlinkError::Savepoint { job_id: job_id.clone(), cause }))?;

                Result::<_, FlinkError>::Ok((job_id, location))
            })
            .collect();

        let failed = failed_statuses
            .into_iter()
            .flat_map(|(job_id, status)| {
                let failure = status
                    .operation
                    .ok_or_else(|| FlinkError::UnexpectedValue {
                        expected: "failed savepoint status must have an operation".to_string(),
                        given: "<none>".to_string(),
                    })?
                    .either(
                        |loc| {
                            Err(FlinkError::UnexpectedValue {
                                expected: "failed savepoint status must have a failure reason, not a location"
                                    .to_string(),
                                given: loc.to_string(),
                            })
                        },
                        Ok,
                    )?;

                Result::<_, FlinkError>::Ok((job_id, failure))
            })
            .collect();

        Ok(Self { completed, failed })
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
    pub operation: Option<Either<SavepointLocation, FailureCause>>,
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
#[serde(rename_all = "kebab-case")]
pub struct FailureCause {
    pub class: String,
    pub stack_trace: String,
}

impl FailureCause {
    pub fn new(class: impl Into<String>, stack_trace: impl Into<String>) -> Self {
        Self {
            class: class.into(),
            stack_trace: stack_trace.into(),
        }
    }
}

impl fmt::Display for FailureCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.class)
    }
}

impl From<FailureCause> for TelemetryValue {
    fn from(reason: FailureCause) -> Self {
        let tv: HashMap<String, Self> = maplit::hashmap! {
            "class".to_string() => reason.class.into(),
            "stack_trace".to_string() => reason.stack_trace.into(),
        };

        tv.into()
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RestoreMode {
    /// Flink will take ownership of the given snapshot. It will clean the snapshot once it is
    /// subsumed by newer ones.
    Claim,

    /// Flink will not claim ownership of the snapshot files. However it will make sure it does
    /// not depend on any artifacts from the restored snapshot. In order to do that, Flink will
    /// take the first checkpoint as a full one, which means it might reupload/duplicate files
    /// that are part of the restored checkpoint.
    NoClaim,

    /// This is the mode in which Flink worked so far. It will not claim ownership of the
    /// snapshot and will not delete the files. However, it can directly depend on the existence
    /// of the files of the restored checkpoint. It might not be safe to delete checkpoints that
    /// were restored in legacy mode.
    Legacy,
}

impl fmt::Display for RestoreMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Claim => write!(f, "CLAIM"),
            Self::NoClaim => write!(f, "NO-CLAIM"),
            Self::Legacy => write!(f, "LEGACY"),
        }
    }
}

impl std::str::FromStr for RestoreMode {
    type Err = FlinkError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "CLAIM" => Ok(Self::Claim),
            "NO-CLAIM" => Ok(Self::NoClaim),
            "LEGACY" => Ok(Self::Legacy),
            _ => Err(FlinkError::UnexpectedValue {
                expected: "CLAIM, NO-CLAIM, LEGACY".to_string(),
                given: s.to_string(),
            }),
        }
    }
}
