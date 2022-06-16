use std::collections::HashSet;
use std::fmt;
use std::ops::Add;

use frunk::{Monoid, Semigroup};
use once_cell::sync::Lazy;
use oso::{Oso, PolarClass};
use pretty_snowflake::{Id, Label, Labeling};
use proctor::elements::telemetry::UpdateMetricsFn;
use proctor::elements::{telemetry, PolicyContributor, Telemetry, Timestamp};
use proctor::error::{PolicyError, ProctorError};
use proctor::phases::sense::SubscriptionRequirements;
use proctor::Correlation;
use prometheus::{Gauge, IntGauge};
use serde::{Deserialize, Serialize};

use crate::metrics::UpdateMetrics;

pub static EMPTY_METRIC_CATALOG: Lazy<MetricCatalog> = Lazy::new(MetricCatalog::empty);

// #[serde_as]
#[derive(PolarClass, Label, PartialEq, Clone, Serialize, Deserialize)]
pub struct MetricCatalog {
    pub correlation_id: Id<Self>,

    #[polar(attribute)]
    pub recv_timestamp: Timestamp,

    #[polar(attribute)]
    #[serde(flatten)] // current subscription mechanism only supports flatten keys
    pub health: JobHealthMetrics,

    #[polar(attribute)]
    #[serde(flatten)] // current subscription mechanism only supports flatten keys
    pub flow: FlowMetrics,

    #[polar(attribute)]
    #[serde(flatten)] // current subscription mechanism only supports flatten keys
    pub cluster: ClusterMetrics,

    #[polar(attribute)]
    #[serde(flatten)] // flatten to collect extra properties.
    pub custom: telemetry::TableType,
}

impl fmt::Debug for MetricCatalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetricCatalog")
            .field("correlation", &self.correlation_id)
            .field("recv_timestamp", &self.recv_timestamp.to_string())
            .field("health", &self.health)
            .field("flow", &self.flow)
            .field("cluster", &self.cluster)
            .field("custom", &self.custom)
            .finish()
    }
}

impl Correlation for MetricCatalog {
    type Correlated = Self;

    fn correlation(&self) -> &Id<Self::Correlated> {
        &self.correlation_id
    }
}

impl PolicyContributor for MetricCatalog {
    #[tracing::instrument(level = "trace", skip(engine))]
    fn register_with_policy_engine(engine: &mut Oso) -> Result<(), PolicyError> {
        engine.register_class(Self::get_polar_class())?;
        engine.register_class(JobHealthMetrics::get_polar_class())?;
        engine.register_class(
            FlowMetrics::get_polar_class_builder()
                .name("FlowMetrics")
                .add_method("average_task_utilization", FlowMetrics::average_task_utilization)
                .build(),
        )?;
        engine.register_class(
            ClusterMetrics::get_polar_class_builder()
                .name("ClusterMetrics")
                .add_method("task_heap_memory_load", ClusterMetrics::task_heap_memory_load)
                .add_method(
                    "task_network_input_utilization",
                    ClusterMetrics::task_network_input_utilization,
                )
                .add_method(
                    "task_network_output_utilization",
                    ClusterMetrics::task_network_output_utilization,
                )
                .build(),
        )?;
        Ok(())
    }
}

impl Monoid for MetricCatalog {
    fn empty() -> Self {
        Self {
            correlation_id: Id::direct(<Self as Label>::labeler().label(), 0, "<undefined>"),
            recv_timestamp: Timestamp::ZERO,
            health: JobHealthMetrics::empty(),
            flow: FlowMetrics::empty(),
            cluster: ClusterMetrics::empty(),
            custom: telemetry::TableType::new(),
        }
    }
}

impl Semigroup for MetricCatalog {
    fn combine(&self, other: &Self) -> Self {
        let mut custom = self.custom.clone();
        custom.extend(other.custom.clone());

        Self {
            correlation_id: other.correlation_id.clone(),
            recv_timestamp: other.recv_timestamp,
            health: self.health.combine(&other.health),
            flow: self.flow.combine(&other.flow),
            cluster: self.cluster.combine(&other.cluster),
            custom,
        }
    }
}

#[derive(PolarClass, Debug, Default, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct JobHealthMetrics {
    // todo per Flink doc's this metric does not work properly under Reactive mode. remove in favor of eligibility's
    // last_failure?
    /// The time that the job has been running without interruption.
    /// Flink REST API: /jobs/metrics?get=uptime&agg=max
    /// Returns -1 for completed jobs (in milliseconds).
    #[polar(attribute)]
    #[serde(rename = "health.job_uptime_millis")]
    pub job_uptime_millis: i64,

    /// The total number of restarts since this job was submitted, including full restarts and
    /// fine-grained restarts.
    /// Flink REST API: /jobs/metrics?get=numRestarts&agg=max
    #[polar(attribute)]
    #[serde(rename = "health.job_nr_restarts")]
    pub job_nr_restarts: i64,

    /// The number of successfully completed checkpoints.
    /// Note: this metrics does not work properly when Reactive Mode is enabled.
    /// Flink REST API: /jobs/metrics?get=numberOfCompletedCheckpoints&agg=max
    #[polar(attribute)]
    #[serde(rename = "health.job_nr_completed_checkpoints")]
    pub job_nr_completed_checkpoints: i64,

    /// The number of failed checkpoints.
    /// Note: this metrics does not work properly when Reactive Mode is enabled.
    /// Flink REST API: /jobs/metrics?get=numberOfFailedCheckpoints&agg=max
    #[polar(attribute)]
    #[serde(rename = "health.job_nr_failed_checkpoints")]
    pub job_nr_failed_checkpoints: i64,
}

impl Monoid for JobHealthMetrics {
    fn empty() -> Self {
        Self {
            job_uptime_millis: -1,
            job_nr_restarts: -1,
            job_nr_completed_checkpoints: -1,
            job_nr_failed_checkpoints: -1,
        }
    }
}

impl Semigroup for JobHealthMetrics {
    fn combine(&self, other: &Self) -> Self {
        other.clone()
    }
}

pub const MC_FLOW__RECORDS_IN_PER_SEC: &str = "flow.records_in_per_sec";
pub const MC_FLOW__FORECASTED_TIMESTAMP: &str = "flow.forecasted_timestamp";
pub const MC_FLOW__FORECASTED_RECORDS_IN_PER_SEC: &str = "flow.forecasted_records_in_per_sec";

#[derive(PolarClass, Default, PartialEq, Clone, Serialize, Deserialize)]
pub struct FlowMetrics {
    /// max rate of records flow into kafka/kinesis related subtask
    /// Flink REST API:
    /// /jobs/<job-id>/vertices/<vertex-id>/subtasks/metrics?get=numRecordsInPerSecond&subtask=0&
    /// agg=max Flink REST API:
    /// /jobs/<job-id>/vertices/<vertex-id>?get=numRecordsInPerSecond&agg=max
    /// and regex for all subtask.metric fields
    // todo: determine which vertices pertains to kafka/kinesis by:
    #[polar(attribute)]
    #[serde(rename = "flow.records_in_per_sec")]
    pub records_in_per_sec: f64,

    /// max rate of records flow out of job kafka/kinesis related subtask
    /// Flink REST API:
    /// /jobs/<job-id>/vertices/<vertex-id>/subtasks/metrics?get=numRecordsOutPerSecond&subtask=0&
    /// agg=max
    // todo: determine which vertices pertains to kafka/kinesis by:
    #[polar(attribute)]
    #[serde(rename = "flow.records_out_per_sec")]
    pub records_out_per_sec: f64,

    /// average rate of idle time in (ideally non-source) tasks. Currently source tasks are not removed
    /// so this metric is skewed downward.
    #[polar(attribute)]
    #[serde(rename = "flow.idle_time_millis_per_sec")]
    pub idle_time_millis_per_sec: f64,

    /// Timestamp (in fractional secs) for the forecasted_records_in_per_sec value.
    #[polar(attribute)]
    #[serde(
        default,
        rename = "flow.forecasted_timestamp",
        skip_serializing_if = "Option::is_none"
    )]
    pub forecasted_timestamp: Option<Timestamp>,

    /// Forecasted rate of records flow predicted by springline.
    #[polar(attribute)]
    #[serde(
        default,
        rename = "flow.forecasted_records_in_per_sec",
        skip_serializing_if = "Option::is_none"
    )]
    pub forecasted_records_in_per_sec: Option<f64>,

    /// Applies to Kafka input connections. Pulled from the FlinkKafkaConsumer records-lag-max
    /// metric.
    #[polar(attribute)]
    #[serde(
        default,
        rename = "flow.input_records_lag_max",
        skip_serializing_if = "Option::is_none"
    )]
    pub input_records_lag_max: Option<i64>,

    /// Applies to Kinesis input connections. Pulled from the FlinkKinesisConsumer
    /// millisBehindLatest metric.
    #[polar(attribute)]
    #[serde(
        default,
        rename = "flow.millis_behind_latest",
        skip_serializing_if = "Option::is_none"
    )]
    pub input_millis_behind_latest: Option<i64>,
}

impl fmt::Debug for FlowMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut out = f.debug_struct("FlowMetrics");
        out.field("records_out_per_sec", &self.records_out_per_sec);
        out.field("records_in_per_sec", &self.records_in_per_sec);
        out.field("idle_time_millis_per_sec", &self.idle_time_millis_per_sec);

        if let Some((ts, recs_in_rate)) = self.forecasted_timestamp.zip(self.forecasted_records_in_per_sec) {
            out.field("forecasted_timestamp", &ts.to_string());
            out.field("forecasted_records_in_per_sec", &recs_in_rate);
        }

        if let Some(lag) = self.input_records_lag_max {
            out.field("input_records_lag_max", &lag);
        }

        if let Some(behind) = self.input_millis_behind_latest {
            out.field("input_millis_behind_latest", &behind);
        }

        out.finish()
    }
}

impl FlowMetrics {
    pub fn average_task_utilization(&self) -> f64 {
        let idle = self.idle_time_millis_per_sec.max(0.0).min(1_000.0);
        1.0 - idle / 1_000.0
    }
}

impl Monoid for FlowMetrics {
    fn empty() -> Self {
        Self {
            records_in_per_sec: 0.0,
            records_out_per_sec: 0.0,
            idle_time_millis_per_sec: 0.0,
            forecasted_timestamp: None,
            forecasted_records_in_per_sec: None,
            input_records_lag_max: None,
            input_millis_behind_latest: None,
        }
    }
}

impl Semigroup for FlowMetrics {
    fn combine(&self, other: &Self) -> Self {
        other.clone()
    }
}

pub const MC_CLUSTER__NR_ACTIVE_JOBS: &str = "cluster.nr_active_jobs";
pub const MC_CLUSTER__NR_TASK_MANAGERS: &str = "cluster.nr_task_managers";

#[derive(PolarClass, Default, PartialEq, Clone, Serialize, Deserialize)]
pub struct ClusterMetrics {
    /// Count of active jobs returned from Flink REST /jobs endpoint
    #[polar(attribute)]
    #[serde(rename = "cluster.nr_active_jobs")]
    pub nr_active_jobs: u32,

    /// Count of entries returned from Flink REST API /taskmanagers
    #[polar(attribute)]
    #[serde(rename = "cluster.nr_task_managers")]
    pub nr_task_managers: u32,

    /// The recent CPU usage of the JVM for all taskmanagers.
    /// - Flink REST API /taskmanagers/metrics?get=Status.JVM.CPU.LOAD&agg=max
    #[polar(attribute)]
    #[serde(rename = "cluster.task_cpu_load")]
    pub task_cpu_load: f64,

    /// The amount of heap memory currently used (in bytes).
    /// - Flink REST API /taskmanagers/metrics?get=Status.JVM.Memory.Heap.Used&agg=max
    #[polar(attribute)]
    #[serde(rename = "cluster.task_heap_memory_used")]
    pub task_heap_memory_used: f64,

    /// The amount of heap memory guaranteed to be available to the JVM (in bytes).
    /// - Flink REST API /taskmanagers/metrics?get=Status.JVM.Memory.Heap.Committed&agg=max
    #[polar(attribute)]
    #[serde(rename = "cluster.task_heap_memory_committed")]
    pub task_heap_memory_committed: f64,

    /// The total number of live threads.
    /// - Flink REST API /taskmanagers/metrics?get=Status.JVM.Threads.Count&agg=max
    #[polar(attribute)]
    #[serde(rename = "cluster.task_nr_threads")]
    pub task_nr_threads: i64,

    /// The number of queued input buffers.
    #[polar(attribute)]
    #[serde(rename = "cluster.task_network_input_queue_len")]
    pub task_network_input_queue_len: f64,

    /// An estimate of the input buffers usage.
    #[polar(attribute)]
    #[serde(rename = "cluster.task_network_input_pool_usage")]
    pub task_network_input_pool_usage: f64,

    /// The number of queued input buffers.
    #[polar(attribute)]
    #[serde(rename = "cluster.task_network_output_queue_len")]
    pub task_network_output_queue_len: f64,

    /// An estimate of the output buffers usage.
    #[polar(attribute)]
    #[serde(rename = "cluster.task_network_output_pool_usage")]
    pub task_network_output_pool_usage: f64,
}

#[allow(unused_must_use)]
impl ClusterMetrics {
    pub fn task_heap_memory_load(&self) -> f64 {
        if self.task_heap_memory_committed == 0.0 {
            0.0
        } else {
            self.task_heap_memory_used / self.task_heap_memory_committed
        }
    }

    pub fn task_network_input_utilization(&self) -> f64 {
        if self.task_network_input_queue_len == 0.0 {
            0.0
        } else {
            self.task_network_input_pool_usage / self.task_network_input_queue_len
        }
    }

    pub fn task_network_output_utilization(&self) -> f64 {
        if self.task_network_output_queue_len == 0.0 {
            0.0
        } else {
            self.task_network_output_pool_usage / self.task_network_output_queue_len
        }
    }
}

impl fmt::Debug for ClusterMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClusterMetrics")
            .field("nr_active_jobs", &self.nr_active_jobs)
            .field("nr_task_managers", &self.nr_task_managers)
            .field("task_cpu_load", &self.task_cpu_load)
            .field("task_heap_memory_used", &self.task_heap_memory_used)
            .field("task_heap_memory_committed", &self.task_heap_memory_committed)
            .field("task_heap_memory_load", &self.task_heap_memory_load())
            .field("task_nr_threads", &self.task_nr_threads)
            .field("task_network_input_queue_len", &self.task_network_input_queue_len)
            .field("task_network_input_pool_usage", &self.task_network_input_pool_usage)
            .field("task_network_input_utilization", &self.task_network_input_utilization())
            .field("task_network_output_queue_len", &self.task_network_output_queue_len)
            .field("task_network_output_pool_usage", &self.task_network_output_pool_usage)
            .field(
                "task_network_output_utilization",
                &self.task_network_output_utilization(),
            )
            .finish()
    }
}

impl Monoid for ClusterMetrics {
    fn empty() -> Self {
        Self {
            nr_active_jobs: 0,
            nr_task_managers: 0,
            task_cpu_load: -1.0,
            task_heap_memory_used: -1.0,
            task_heap_memory_committed: -1.0,
            task_nr_threads: -1,
            task_network_input_queue_len: -1.0,
            task_network_input_pool_usage: -1.0,
            task_network_output_queue_len: -1.0,
            task_network_output_pool_usage: -1.0,
        }
    }
}

impl Semigroup for ClusterMetrics {
    fn combine(&self, other: &Self) -> Self {
        other.clone()
    }
}

impl Add for MetricCatalog {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        let mut lhs = self.custom;
        lhs.extend(rhs.custom);
        Self { custom: lhs, ..self }
    }
}

impl Add<&Self> for MetricCatalog {
    type Output = Self;

    fn add(self, rhs: &Self) -> Self::Output {
        let mut lhs = self.custom;
        lhs.extend(rhs.custom.clone());
        Self { custom: lhs, ..self }
    }
}

impl SubscriptionRequirements for MetricCatalog {
    fn required_fields() -> HashSet<String> {
        maplit::hashset! {
            // JobHealthMetrics
            "health.job_uptime_millis".into(),
            "health.job_nr_restarts".into(),
            "health.job_nr_completed_checkpoints".into(),
            "health.job_nr_failed_checkpoints".into(),

            // FlowMetrics
            MC_FLOW__RECORDS_IN_PER_SEC.into(),
            "flow.records_out_per_sec".into(),
            "flow.idle_time_millis_per_sec".into(),

            // ClusterMetrics
            MC_CLUSTER__NR_ACTIVE_JOBS.into(),
            MC_CLUSTER__NR_TASK_MANAGERS.into(),
            "cluster.task_cpu_load".into(),
            "cluster.task_heap_memory_used".into(),
            "cluster.task_heap_memory_committed".into(),
            "cluster.task_nr_threads".into(),
            "cluster.task_network_input_queue_len".into(),
            "cluster.task_network_input_pool_usage".into(),
            "cluster.task_network_output_queue_len".into(),
            "cluster.task_network_output_pool_usage".into(),
        }
    }

    fn optional_fields() -> HashSet<String> {
        maplit::hashset! {
            // FlowMetrics
            "flow.forecasted_timestamp".into(),
            "flow.forecasted_records_in_per_sec".into(),
            "flow.input_records_lag_max".into(),
            "flow.input_millis_behind_latest".into(),
        }
    }
}

impl UpdateMetrics for MetricCatalog {
    fn update_metrics_for(name: &str) -> UpdateMetricsFn {
        let phase_name = name.to_string();
        let update_fn = move |subscription_name: &str, telemetry: &Telemetry| match telemetry.clone().try_into::<Self>()
        {
            Ok(catalog) => {
                METRIC_CATALOG_TIMESTAMP.set(catalog.recv_timestamp.as_secs());

                METRIC_CATALOG_JOB_HEALTH_UPTIME.set(catalog.health.job_uptime_millis);
                METRIC_CATALOG_JOB_HEALTH_NR_RESTARTS.set(catalog.health.job_nr_restarts);
                METRIC_CATALOG_JOB_HEALTH_NR_COMPLETED_CHECKPOINTS.set(catalog.health.job_nr_completed_checkpoints);
                METRIC_CATALOG_JOB_HEALTH_NR_FAILED_CHECKPOINTS.set(catalog.health.job_nr_failed_checkpoints);

                METRIC_CATALOG_FLOW_RECORDS_IN_PER_SEC.set(catalog.flow.records_in_per_sec);
                METRIC_CATALOG_FLOW_RECORDS_OUT_PER_SEC.set(catalog.flow.records_out_per_sec);
                METRIC_CATALOG_FLOW_IDLE_TIME_MILLIS_PER_SEC.set(catalog.flow.idle_time_millis_per_sec);

                if let Some(lag) = catalog.flow.input_records_lag_max {
                    METRIC_CATALOG_FLOW_INPUT_RECORDS_LAG_MAX.set(lag);
                }

                if let Some(lag) = catalog.flow.input_millis_behind_latest {
                    METRIC_CATALOG_FLOW_INPUT_MILLIS_BEHIND_LATEST.set(lag);
                }

                METRIC_CATALOG_CLUSTER_NR_ACTIVE_JOBS.set(catalog.cluster.nr_active_jobs as i64);
                METRIC_CATALOG_CLUSTER_NR_TASK_MANAGERS.set(catalog.cluster.nr_task_managers as i64);
                METRIC_CATALOG_CLUSTER_TASK_CPU_LOAD.set(catalog.cluster.task_cpu_load);
                METRIC_CATALOG_CLUSTER_TASK_HEAP_MEMORY_USED.set(catalog.cluster.task_heap_memory_used);
                METRIC_CATALOG_CLUSTER_TASK_HEAP_MEMORY_COMMITTED.set(catalog.cluster.task_heap_memory_committed);
                METRIC_CATALOG_CLUSTER_TASK_NR_THREADS.set(catalog.cluster.task_nr_threads);
                METRIC_CATALOG_CLUSTER_TASK_NETWORK_INPUT_QUEUE_LEN.set(catalog.cluster.task_network_input_queue_len);
                METRIC_CATALOG_CLUSTER_TASK_NETWORK_INPUT_POOL_USAGE.set(catalog.cluster.task_network_input_pool_usage);
                METRIC_CATALOG_CLUSTER_TASK_NETWORK_OUTPUT_QUEUE_LEN.set(catalog.cluster.task_network_output_queue_len);
                METRIC_CATALOG_CLUSTER_TASK_NETWORK_OUTPUT_POOL_USAGE
                    .set(catalog.cluster.task_network_output_pool_usage);
            },

            Err(err) => {
                tracing::warn!(
                    error=?err, %phase_name,
                    "failed to update sensor metrics for subscription: {}", subscription_name
                );
                proctor::track_errors(&phase_name, &ProctorError::SensePhase(err.into()));
            },
        };

        Box::new(update_fn)
    }
}

pub static METRIC_CATALOG_TIMESTAMP: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "metric_catalog_timestamp",
        "UNIX timestamp in seconds of last operational reading",
    )
    .expect("failed creating metric_catalog_timestamp metric")
});

pub static METRIC_CATALOG_JOB_HEALTH_UPTIME: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "metric_catalog_job_health_uptime",
        "The time that the job has been running without interruption.",
    )
    .expect("failed creating metric_catalog_job_health_uptime metric")
});

pub static METRIC_CATALOG_JOB_HEALTH_NR_RESTARTS: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "metric_catalog_job_health_nr_restarts",
        "The total number of restarts since this job was submitted, including full restarts and fine-grained restarts.",
    )
    .expect("failed creating metric_catalog_job_health_nr_restarts metric")
});

pub static METRIC_CATALOG_JOB_HEALTH_NR_COMPLETED_CHECKPOINTS: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "metric_catalog_job_health_nr_completed_checkpoints",
        "The number of successfully completed checkpoints.",
    )
    .expect("failed creating metric_catalog_job_health_nr_completed_checkpoints metric")
});

pub static METRIC_CATALOG_JOB_HEALTH_NR_FAILED_CHECKPOINTS: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "metric_catalog_job_health_nr_failed_checkpoints",
        "The number of failed checkpoints.",
    )
    .expect("failed creating metric_catalog_job_health_nr_failed_checkpoints metric")
});

pub static METRIC_CATALOG_FLOW_RECORDS_IN_PER_SEC: Lazy<Gauge> = Lazy::new(|| {
    Gauge::new(
        "metric_catalog_flow_records_in_per_sec",
        "Current records ingress per second",
    )
    .expect("failed creating metric_catalog_flow_records_in_per_sec metric")
});

pub static METRIC_CATALOG_FLOW_RECORDS_OUT_PER_SEC: Lazy<Gauge> = Lazy::new(|| {
    Gauge::new(
        "metric_catalog_flow_records_out_per_sec",
        "Current records egress per second",
    )
    .expect("failed creating metric_catalog_flow_records_out_per_sec metric")
});

pub static METRIC_CATALOG_FLOW_IDLE_TIME_MILLIS_PER_SEC: Lazy<Gauge> = Lazy::new(|| {
    Gauge::new(
        "metric_catalog_flow_idle_time_millis_per_sec",
        "Current average idle time in millis per second",
    )
    .expect("failed creating metric_catalog_flow_idle_time_millis_per_sec metric")
});

pub static METRIC_CATALOG_FLOW_INPUT_RECORDS_LAG_MAX: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "metric_catalog_flow_input_records_lag_max",
        "Current lag in handling messages from the Kafka ingress topic",
    )
    .expect("failed creating metric_catalog_flow_input_records_lag_max metric")
});

pub static METRIC_CATALOG_FLOW_INPUT_MILLIS_BEHIND_LATEST: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "metric_catalog_flow_input_millis_behind_latest",
        "Current lag in handling messages from the Kinesis ingress topic",
    )
    .expect("failed creating metric_catalog_flow_input_records_lag_max metric")
});

pub static METRIC_CATALOG_CLUSTER_NR_ACTIVE_JOBS: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "metric_catalog_cluster_nr_active_jobs",
        "Number of active jobs in the cluster",
    )
    .expect("failed creating metric_catalog_cluster_nr_active_jobs metric")
});

pub static METRIC_CATALOG_CLUSTER_NR_TASK_MANAGERS: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "metric_catalog_cluster_nr_task_managers",
        "Number of active task managers in the cluster",
    )
    .expect("failed creating metric_catalog_cluster_nr_task_managers metric")
});

pub static METRIC_CATALOG_CLUSTER_TASK_CPU_LOAD: Lazy<Gauge> = Lazy::new(|| {
    Gauge::new(
        "metric_catalog_cluster_task_cpu_load",
        "The recent CPU usage of the JVM.",
    )
    .expect("failed creating metric_catalog_cluster_task_cpu_load metric")
});

pub static METRIC_CATALOG_CLUSTER_TASK_HEAP_MEMORY_USED: Lazy<Gauge> = Lazy::new(|| {
    Gauge::new(
        "metric_catalog_cluster_task_heap_memory_used",
        "The amount of heap memory currently used (in bytes).",
    )
    .expect("failed creating metric_catalog_cluster_task_heap_memory_used metric")
});

pub static METRIC_CATALOG_CLUSTER_TASK_HEAP_MEMORY_COMMITTED: Lazy<Gauge> = Lazy::new(|| {
    Gauge::new(
        "metric_catalog_cluster_task_heap_memory_committed",
        "The amount of heap memory guaranteed to be available to the JVM (in bytes).",
    )
    .expect("failed creating metric_catalog_cluster_task_heap_memory_committed metric")
});

pub static METRIC_CATALOG_CLUSTER_TASK_NR_THREADS: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "metric_catalog_cluster_task_nr_threads",
        "The total number of live threads.",
    )
    .expect("failed creating metric_catalog_cluster_task_nr_threads metric")
});

pub static METRIC_CATALOG_CLUSTER_TASK_NETWORK_INPUT_QUEUE_LEN: Lazy<Gauge> = Lazy::new(|| {
    Gauge::new(
        "metric_catalog_cluster_task_network_input_queue_len",
        "The number of queued input buffers.",
    )
    .expect("failed creating metric_catalog_cluster_task_network_input_queue_len metric")
});

pub static METRIC_CATALOG_CLUSTER_TASK_NETWORK_INPUT_POOL_USAGE: Lazy<Gauge> = Lazy::new(|| {
    Gauge::new(
        "metric_catalog_cluster_task_network_input_pool_usage",
        "An estimate of the input buffers usage. ",
    )
    .expect("failed creating metric_catalog_cluster_task_network_input_pool_usage metric")
});

pub static METRIC_CATALOG_CLUSTER_TASK_NETWORK_OUTPUT_QUEUE_LEN: Lazy<Gauge> = Lazy::new(|| {
    Gauge::new(
        "metric_catalog_cluster_task_network_output_queue_len",
        "The number of queued output buffers.",
    )
    .expect("failed creating metric_catalog_cluster_task_network_output_queue_len metric")
});

pub static METRIC_CATALOG_CLUSTER_TASK_NETWORK_OUTPUT_POOL_USAGE: Lazy<Gauge> = Lazy::new(|| {
    Gauge::new(
        "metric_catalog_cluster_task_network_output_pool_usage",
        "An estimate of the output buffers usage. ",
    )
    .expect("failed creating metric_catalog_cluster_task_network_output_pool_usage metric")
});
