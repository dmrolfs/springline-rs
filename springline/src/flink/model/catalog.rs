use std::collections::HashSet;
use std::fmt;
use std::ops::Add;

use frunk::{Monoid, Semigroup};
use once_cell::sync::{Lazy, OnceCell};
use oso::{Oso, PolarClass};
use pretty_snowflake::{Id, Label, Labeling};
use proctor::elements::telemetry::UpdateMetricsFn;
use proctor::elements::{telemetry, PolicyContributor, Telemetry, Timestamp};
use proctor::error::{PolicyError, ProctorError};
use proctor::phases::sense::SubscriptionRequirements;
use proctor::{Correlation, ReceivedAt};
use prometheus::{Gauge, IntGauge, Opts};
use serde::{Deserialize, Serialize};

use crate::metrics::UpdateMetrics;

pub static SUPPLEMENTAL_TELEMETRY: OnceCell<HashSet<String>> = OnceCell::new();

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

impl ReceivedAt for MetricCatalog {
    fn recv_timestamp(&self) -> Timestamp {
        self.recv_timestamp
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
                .add_method("task_utilization", FlowMetrics::task_utilization)
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

pub const MC_HEALTH__JOB_MAX_PARALLELISM: &str = "health.job_max_parallelism";

#[derive(PolarClass, Debug, Default, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct JobHealthMetrics {
    /// Max parallelism found in Job. If this is below the number of task managers, then an up
    /// rescale plan may simply restart at an increased parallelism up to the number of task
    /// managers.
    #[polar(attribute)]
    #[serde(default, rename = "health.job_max_parallelism")]
    pub job_max_parallelism: u32,

    // todo per Flink doc's this metric does not work properly under Reactive mode. remove in favor of eligibility's
    // last_failure?
    /// The time that the job has been running without interruption.
    /// Flink REST API: /jobs/metrics?get=uptime&agg=max
    /// Returns -1 for completed jobs (in milliseconds).
    #[polar(attribute)]
    #[serde(rename = "health.job_uptime_millis")]
    pub job_uptime_millis: u32,

    /// The total number of restarts since this job was submitted, including full restarts and
    /// fine-grained restarts.
    /// Flink REST API: /jobs/metrics?get=numRestarts&agg=max
    #[polar(attribute)]
    #[serde(rename = "health.job_nr_restarts")]
    pub job_nr_restarts: u32,

    /// The number of successfully completed checkpoints.
    /// Note: this metrics does not work properly when Reactive Mode is enabled.
    /// Flink REST API: /jobs/metrics?get=numberOfCompletedCheckpoints&agg=max
    #[polar(attribute)]
    #[serde(rename = "health.job_nr_completed_checkpoints")]
    pub job_nr_completed_checkpoints: u32,

    /// The number of failed checkpoints.
    /// Note: this metrics does not work properly when Reactive Mode is enabled.
    /// Flink REST API: /jobs/metrics?get=numberOfFailedCheckpoints&agg=max
    #[polar(attribute)]
    #[serde(rename = "health.job_nr_failed_checkpoints")]
    pub job_nr_failed_checkpoints: u32,
}

impl Monoid for JobHealthMetrics {
    fn empty() -> Self {
        Self {
            job_max_parallelism: 0,
            job_uptime_millis: 0,
            job_nr_restarts: 0,
            job_nr_completed_checkpoints: 0,
            job_nr_failed_checkpoints: 0,
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
    #[polar(attribute)]
    #[serde(rename = "flow.records_in_per_sec")]
    pub records_in_per_sec: f64,

    /// max rate of records flow out of job kafka/kinesis related subtask
    /// Flink REST API:
    /// /jobs/<job-id>/vertices/<vertex-id>/subtasks/metrics?get=numRecordsOutPerSecond&subtask=0&
    /// agg=max
    #[polar(attribute)]
    #[serde(rename = "flow.records_out_per_sec")]
    pub records_out_per_sec: f64,

    /// average rate of idle time in non-source tasks.
    #[polar(attribute)]
    #[serde(rename = "flow.idle_time_millis_per_sec")]
    pub idle_time_millis_per_sec: f64,

    /// The average time (in milliseconds) this source tasks are back pressured per second.
    #[polar(attribute)]
    #[serde(default, rename = "flow.source_back_pressured_time_millis_per_sec")]
    pub source_back_pressured_time_millis_per_sec: f64,

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

    /// Applies to Kafka source connections. Pulled from the FlinkKafkaConsumer records-lag-max
    /// metric.
    #[polar(attribute)]
    #[serde(
        default,
        rename = "flow.source_records_lag_max",
        skip_serializing_if = "Option::is_none"
    )]
    pub source_records_lag_max: Option<u32>,

    #[polar(attribute)]
    #[serde(
        default,
        rename = "flow.source_assigned_partitions",
        skip_serializing_if = "Option::is_none"
    )]
    pub source_assigned_partitions: Option<u32>,

    #[polar(attribute)]
    #[serde(default, rename = "flow.source_total_lag", skip_serializing_if = "Option::is_none")]
    pub source_total_lag: Option<u32>,

    /// The rate at which the job processes records, which is calculated by summing the
    /// `records_consumed_rate` for each operator instance of the Kafka Source. This metric is used
    /// with `total_lag` to calculate `AppDataWindow<MetricCatalog>::relative_lag_change_rate`.
    #[polar(attribute)]
    #[serde(rename = "flow.source_records_consumed_rate")]
    pub source_records_consumed_rate: Option<f64>,

    /// Applies to Kinesis source connections. Pulled from the FlinkKinesisConsumer
    /// millisBehindLatest metric.
    #[polar(attribute)]
    #[serde(
        default,
        rename = "flow.millis_behind_latest",
        skip_serializing_if = "Option::is_none"
    )]
    pub source_millis_behind_latest: Option<u32>,
}

impl fmt::Debug for FlowMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut out = f.debug_struct("FlowMetrics");
        out.field("records_out_per_sec", &self.records_out_per_sec);
        out.field("records_in_per_sec", &self.records_in_per_sec);
        out.field("idle_time_millis_per_sec", &self.idle_time_millis_per_sec);
        out.field(
            "source_back_pressured_time_millis_per_sec",
            &self.source_back_pressured_time_millis_per_sec,
        );
        out.field("task_utilization", &self.task_utilization());

        if let Some((ts, recs_in_rate)) = self.forecasted_timestamp.zip(self.forecasted_records_in_per_sec) {
            out.field("forecasted_timestamp", &ts.to_string());
            out.field("forecasted_records_in_per_sec", &recs_in_rate);
        }

        if let Some(lag) = self.source_records_lag_max {
            out.field("source_records_lag_max", &lag);
        }

        if let Some(partitions) = self.source_assigned_partitions {
            out.field("source_assigned_partitions", &partitions);
        }

        if let Some(total_lag) = self.source_total_lag {
            out.field("source_total_lag", &total_lag);
        }

        if let Some(total_rate) = self.source_records_consumed_rate {
            out.field("source_records_consumed_rate", &total_rate);
        }

        if let Some(behind) = self.source_millis_behind_latest {
            out.field("source_millis_behind_latest", &behind);
        }

        out.finish()
    }
}

impl FlowMetrics {
    /// If a task is idle due to a lack of incoming records, then the job is over-provisioned for
    /// the current load. Utilization is the average portion of time the job tasks spend in a
    /// non-idle state. To be useful, it is important to exclude source operators from the
    /// calculation of this metric (e.g., via a non_source position specifier in the Operator metric
    /// order).
    ///
    /// Utilization is calculated based on the idle_time_millis_per_sec metric:
    ///     1 - average(idle_time_millis_per_sec) / 1_000
    ///
    /// This metric should be used for downscaling decisions only. If utilization is low and
    /// total_lag is 0, the job is able to process more records than the incoming rate.
    ///
    /// (For upscaling, it may trigger a scale up before a lag metric.)
    pub fn task_utilization(&self) -> f64 {
        let idle = self.idle_time_millis_per_sec.max(0.0).min(1_000.0);
        1.0 - idle / 1_000.0
    }

    pub fn source_back_pressure_percentage(&self) -> f64 {
        let backpressure = self.source_back_pressured_time_millis_per_sec.max(0.0).min(1_000.0);
        backpressure / 1_000.0
    }
}

impl Monoid for FlowMetrics {
    fn empty() -> Self {
        Self {
            records_in_per_sec: 0.0,
            records_out_per_sec: 0.0,
            idle_time_millis_per_sec: 0.0,
            source_back_pressured_time_millis_per_sec: 0.0,
            forecasted_timestamp: None,
            forecasted_records_in_per_sec: None,
            source_records_lag_max: None,
            source_assigned_partitions: None,
            source_total_lag: None,
            source_records_consumed_rate: None,
            source_millis_behind_latest: None,
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
    pub task_nr_threads: u32,

    /// The number of queued input buffers.
    #[polar(attribute)]
    #[serde(rename = "cluster.task_network_input_queue_len")]
    pub task_network_input_queue_len: f64,

    /// An estimate of the input buffers usage.
    #[polar(attribute)]
    #[serde(rename = "cluster.task_network_input_pool_usage")]
    pub task_network_input_pool_usage: f64,

    /// The number of queued output buffers.
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
            task_nr_threads: 0,
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
            MC_HEALTH__JOB_MAX_PARALLELISM.into(),
            "health.job_uptime_millis".into(),
            "health.job_nr_restarts".into(),
            "health.job_nr_completed_checkpoints".into(),
            "health.job_nr_failed_checkpoints".into(),

            // FlowMetrics
            MC_FLOW__RECORDS_IN_PER_SEC.into(),
            "flow.records_out_per_sec".into(),
            "flow.idle_time_millis_per_sec".into(),
            "flow.source_back_pressured_time_millis_per_sec".into(),

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
        let mut optional = maplit::hashset! {
            // FlowMetrics
            "flow.forecasted_timestamp".into(),
            "flow.forecasted_records_in_per_sec".into(),
            // "flow.source_records_lag_max".into(),
            // "flow.source_assigned_partitions".into(),
            // "flow.source_total_lag".into(),
            // "flow.source_records_consumed_rate".into(),
            // "flow.source_millis_behind_latest".into(),
        };

        if let Some(supplemental) = SUPPLEMENTAL_TELEMETRY.get().cloned() {
            tracing::info!(
                ?supplemental,
                "adding supplemental telemetry to MetricCatalog optional fields subscription."
            );
            optional.extend(supplemental);
        }

        optional
    }
}

impl UpdateMetrics for MetricCatalog {
    fn update_metrics_for(name: &str) -> UpdateMetricsFn {
        let phase_name = name.to_string();
        let update_fn = move |subscription_name: &str, telemetry: &Telemetry| match telemetry.clone().try_into::<Self>()
        {
            Ok(catalog) => {
                METRIC_CATALOG_TIMESTAMP.set(catalog.recv_timestamp.as_secs());

                METRIC_CATALOG_JOB_HEALTH_UPTIME.set(catalog.health.job_uptime_millis.into());
                METRIC_CATALOG_JOB_HEALTH_NR_RESTARTS.set(catalog.health.job_nr_restarts.into());
                METRIC_CATALOG_JOB_HEALTH_NR_COMPLETED_CHECKPOINTS
                    .set(catalog.health.job_nr_completed_checkpoints.into());
                METRIC_CATALOG_JOB_HEALTH_NR_FAILED_CHECKPOINTS.set(catalog.health.job_nr_failed_checkpoints.into());

                METRIC_CATALOG_FLOW_RECORDS_IN_PER_SEC.set(catalog.flow.records_in_per_sec);
                METRIC_CATALOG_FLOW_RECORDS_OUT_PER_SEC.set(catalog.flow.records_out_per_sec);
                METRIC_CATALOG_FLOW_IDLE_TIME_MILLIS_PER_SEC.set(catalog.flow.idle_time_millis_per_sec);
                METRIC_CATALOG_FLOW_SOURCE_BACK_PRESSURE_TIME_MILLIS_PER_SEC
                    .set(catalog.flow.source_back_pressured_time_millis_per_sec);
                METRIC_CATALOG_FLOW_TASK_UTILIZATION.set(catalog.flow.task_utilization());

                if let Some(lag) = catalog.flow.source_records_lag_max {
                    METRIC_CATALOG_FLOW_SOURCE_RECORDS_LAG_MAX.set(lag.into());
                }

                if let Some(partitions) = catalog.flow.source_assigned_partitions {
                    METRIC_CATALOG_FLOW_SOURCE_ASSIGNED_PARTITIONS.set(partitions.into());
                }

                if let Some(total_lag) = catalog.flow.source_total_lag {
                    METRIC_CATALOG_FLOW_SOURCE_TOTAL_LAG.set(total_lag.into());
                }

                if let Some(total_rate) = catalog.flow.source_records_consumed_rate {
                    METRIC_CATALOG_FLOW_SOURCE_RECORDS_CONSUMED_RATE.set(total_rate);
                }

                if let Some(lag) = catalog.flow.source_millis_behind_latest {
                    METRIC_CATALOG_FLOW_SOURCE_MILLIS_BEHIND_LATEST.set(lag.into());
                }

                METRIC_CATALOG_CLUSTER_NR_ACTIVE_JOBS.set(catalog.cluster.nr_active_jobs.into());
                METRIC_CATALOG_CLUSTER_NR_TASK_MANAGERS.set(catalog.cluster.nr_task_managers.into());
                METRIC_CATALOG_CLUSTER_TASK_CPU_LOAD.set(catalog.cluster.task_cpu_load);
                METRIC_CATALOG_CLUSTER_TASK_HEAP_MEMORY_USED.set(catalog.cluster.task_heap_memory_used);
                METRIC_CATALOG_CLUSTER_TASK_HEAP_MEMORY_COMMITTED.set(catalog.cluster.task_heap_memory_committed);
                METRIC_CATALOG_CLUSTER_TASK_NR_THREADS.set(catalog.cluster.task_nr_threads.into());
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
    IntGauge::with_opts(
        Opts::new(
            "metric_catalog_timestamp",
            "UNIX timestamp in seconds of last operational reading",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating metric_catalog_timestamp metric")
});

pub static METRIC_CATALOG_JOB_HEALTH_UPTIME: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::with_opts(
        Opts::new(
            "metric_catalog_job_health_uptime",
            "The time that the job has been running without interruption.",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating metric_catalog_job_health_uptime metric")
});

pub static METRIC_CATALOG_JOB_HEALTH_NR_RESTARTS: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::with_opts(
        Opts::new(
        "metric_catalog_job_health_nr_restarts",
        "The total number of restarts since this job was submitted, including full restarts and fine-grained restarts.",
        )
            .const_labels(proctor::metrics::CONST_LABELS.clone()))
    .expect("failed creating metric_catalog_job_health_nr_restarts metric")
});

pub static METRIC_CATALOG_JOB_HEALTH_NR_COMPLETED_CHECKPOINTS: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::with_opts(
        Opts::new(
            "metric_catalog_job_health_nr_completed_checkpoints",
            "The number of successfully completed checkpoints.",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating metric_catalog_job_health_nr_completed_checkpoints metric")
});

pub static METRIC_CATALOG_JOB_HEALTH_NR_FAILED_CHECKPOINTS: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::with_opts(
        Opts::new(
            "metric_catalog_job_health_nr_failed_checkpoints",
            "The number of failed checkpoints.",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating metric_catalog_job_health_nr_failed_checkpoints metric")
});

pub static METRIC_CATALOG_FLOW_RECORDS_IN_PER_SEC: Lazy<Gauge> = Lazy::new(|| {
    Gauge::with_opts(
        Opts::new(
            "metric_catalog_flow_records_in_per_sec",
            "Current records ingress per second",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating metric_catalog_flow_records_in_per_sec metric")
});

pub static METRIC_CATALOG_FLOW_RECORDS_OUT_PER_SEC: Lazy<Gauge> = Lazy::new(|| {
    Gauge::with_opts(
        Opts::new(
            "metric_catalog_flow_records_out_per_sec",
            "Current records egress per second",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating metric_catalog_flow_records_out_per_sec metric")
});

pub static METRIC_CATALOG_FLOW_IDLE_TIME_MILLIS_PER_SEC: Lazy<Gauge> = Lazy::new(|| {
    Gauge::with_opts(
        Opts::new(
            "metric_catalog_flow_idle_time_millis_per_sec",
            "Current average idle time in millis per second",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating metric_catalog_flow_idle_time_millis_per_sec metric")
});

pub static METRIC_CATALOG_FLOW_SOURCE_BACK_PRESSURE_TIME_MILLIS_PER_SEC: Lazy<Gauge> = Lazy::new(|| {
    Gauge::with_opts(
        Opts::new(
            "metric_catalog_flow_source_back_pressure_time_millis_per_sec",
            "Average source back pressure time in millis per second",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating metric_catalog_flow_back_pressure_time_millis_per_sec metric")
});

pub static METRIC_CATALOG_FLOW_TASK_UTILIZATION: Lazy<Gauge> = Lazy::new(|| {
    Gauge::with_opts(
        Opts::new(
            "metric_catalog_flow_task_utilization",
            "Current average task utilization",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating metric_catalog_flow_task_utilization metric")
});

pub static METRIC_CATALOG_FLOW_SOURCE_RECORDS_LAG_MAX: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::with_opts(
        Opts::new(
            "metric_catalog_flow_source_records_lag_max",
            "Current lag in handling messages from the Kafka ingress topic",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating metric_catalog_flow_source_records_lag_max metric")
});

pub static METRIC_CATALOG_FLOW_SOURCE_ASSIGNED_PARTITIONS: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::with_opts(
        Opts::new(
            "metric_catalog_flow_source_assigned_partitions",
            "total assigned partitions in handling messages from the Kafka ingress topic",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating metric_catalog_flow_source_assigned_partitions metric")
});

pub static METRIC_CATALOG_FLOW_SOURCE_TOTAL_LAG: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::with_opts(
        Opts::new(
            "metric_catalog_flow_source_total_lag",
            "total estimated lag considering partitions in handling messages from the Kafka ingress topic",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating metric_catalog_flow_source_total_lag metric")
});

pub static METRIC_CATALOG_FLOW_SOURCE_RECORDS_CONSUMED_RATE: Lazy<Gauge> = Lazy::new(|| {
    Gauge::with_opts(
        Opts::new(
            "metric_catalog_flow_source_records_consumed_rate",
            "total rate at which the job processes records from Kafka.",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating metric_catalog_flow_source_records_consumed_rate metric")
});

pub static METRIC_CATALOG_FLOW_SOURCE_MILLIS_BEHIND_LATEST: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::with_opts(
        Opts::new(
            "metric_catalog_flow_source_millis_behind_latest",
            "Current lag in handling messages from the Kinesis ingress topic",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating metric_catalog_flow_source_records_lag_max metric")
});

pub static METRIC_CATALOG_CLUSTER_NR_ACTIVE_JOBS: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::with_opts(
        Opts::new(
            "metric_catalog_cluster_nr_active_jobs",
            "Number of active jobs in the cluster",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating metric_catalog_cluster_nr_active_jobs metric")
});

pub static METRIC_CATALOG_CLUSTER_NR_TASK_MANAGERS: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::with_opts(
        Opts::new(
            "metric_catalog_cluster_nr_task_managers",
            "Number of active task managers in the cluster",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating metric_catalog_cluster_nr_task_managers metric")
});

pub static METRIC_CATALOG_CLUSTER_TASK_CPU_LOAD: Lazy<Gauge> = Lazy::new(|| {
    Gauge::with_opts(
        Opts::new(
            "metric_catalog_cluster_task_cpu_load",
            "The recent CPU usage of the JVM.",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating metric_catalog_cluster_task_cpu_load metric")
});

pub static METRIC_CATALOG_CLUSTER_TASK_HEAP_MEMORY_USED: Lazy<Gauge> = Lazy::new(|| {
    Gauge::with_opts(
        Opts::new(
            "metric_catalog_cluster_task_heap_memory_used",
            "The amount of heap memory currently used (in bytes).",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating metric_catalog_cluster_task_heap_memory_used metric")
});

pub static METRIC_CATALOG_CLUSTER_TASK_HEAP_MEMORY_COMMITTED: Lazy<Gauge> = Lazy::new(|| {
    Gauge::with_opts(
        Opts::new(
            "metric_catalog_cluster_task_heap_memory_committed",
            "The amount of heap memory guaranteed to be available to the JVM (in bytes).",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating metric_catalog_cluster_task_heap_memory_committed metric")
});

pub static METRIC_CATALOG_CLUSTER_TASK_NR_THREADS: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::with_opts(
        Opts::new(
            "metric_catalog_cluster_task_nr_threads",
            "The total number of live threads.",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating metric_catalog_cluster_task_nr_threads metric")
});

pub static METRIC_CATALOG_CLUSTER_TASK_NETWORK_INPUT_QUEUE_LEN: Lazy<Gauge> = Lazy::new(|| {
    Gauge::with_opts(
        Opts::new(
            "metric_catalog_cluster_task_network_input_queue_len",
            "The number of queued input buffers.",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating metric_catalog_cluster_task_network_input_queue_len metric")
});

pub static METRIC_CATALOG_CLUSTER_TASK_NETWORK_INPUT_POOL_USAGE: Lazy<Gauge> = Lazy::new(|| {
    Gauge::with_opts(
        Opts::new(
            "metric_catalog_cluster_task_network_input_pool_usage",
            "An estimate of the input buffers usage. ",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating metric_catalog_cluster_task_network_input_pool_usage metric")
});

pub static METRIC_CATALOG_CLUSTER_TASK_NETWORK_OUTPUT_QUEUE_LEN: Lazy<Gauge> = Lazy::new(|| {
    Gauge::with_opts(
        Opts::new(
            "metric_catalog_cluster_task_network_output_queue_len",
            "The number of queued output buffers.",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating metric_catalog_cluster_task_network_output_queue_len metric")
});

pub static METRIC_CATALOG_CLUSTER_TASK_NETWORK_OUTPUT_POOL_USAGE: Lazy<Gauge> = Lazy::new(|| {
    Gauge::with_opts(
        Opts::new(
            "metric_catalog_cluster_task_network_output_pool_usage",
            "An estimate of the output buffers usage. ",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating metric_catalog_cluster_task_network_output_pool_usage metric")
});
