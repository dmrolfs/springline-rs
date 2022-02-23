use cast_trait_object::DynCastExt;
use once_cell::sync::Lazy;
use proctor::elements::telemetry::{self, Telemetry, TelemetryValue};
use proctor::error::{MetricLabel, SenseError, TelemetryError};
use proctor::graph::stage::{self, SourceStage, ThroughStage};
use proctor::graph::{Connect, Graph, SinkShape, SourceShape, UniformFanInShape, UniformFanOutShape};
use proctor::SharedString;
use prometheus::{HistogramOpts, HistogramTimer, HistogramVec, IntCounterVec, Opts};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::policies::ExponentialBackoff;
use reqwest_retry::RetryTransientMiddleware;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use url::Url;

mod api_model;
mod metric_order;
mod scope_sensor;
mod taskmanager_admin_sensor;
mod vertex_sensor;

// #[allow(dead_code)]
// mod generators;

use crate::phases::MC_FLOW__RECORDS_IN_PER_SEC;
pub use vertex_sensor::{
    FLINK_ACTIVE_JOBS_SENSOR_TIME, FLINK_QUERY_JOB_DETAIL_TIME, FLINK_VERTEX_SENSOR_AVAIL_TELEMETRY_TIME,
    FLINK_VERTEX_SENSOR_METRIC_PICKLIST_TIME, FLINK_VERTEX_SENSOR_TIME,
};
// pub use generators::make_flink_metrics_source;
use crate::phases::sense::flink::scope_sensor::ScopeSensor;
use crate::phases::sense::flink::taskmanager_admin_sensor::TaskmanagerAdminSensor;
use crate::phases::sense::flink::vertex_sensor::VertexSensor;
use crate::settings::FlinkSettings;
pub use api_model::{JobId, JobState, TaskState, VertexId, JOB_STATES, TASK_STATES};
pub use metric_order::{Aggregation, FlinkScope, MetricOrder};

// note: `cluster.nr_task_managers` is a standard metric pulled from Flink's admin API. The order
// mechanism may need to be expanded to consider further meta information outside of Flink Metrics
// API.
pub static STD_METRIC_ORDERS: Lazy<Vec<MetricOrder>> = Lazy::new(|| {
    use proctor::elements::TelemetryType::*;

    use self::{Aggregation::*, FlinkScope::*};

    [
        (Jobs, "uptime", Max, "health.job_uptime_millis", Integer), // does not work w reactive mode
        (Jobs, "numRestarts", Max, "health.job_nr_restarts", Integer),
        (
            Jobs,
            "numberOfCompletedCheckpoints",
            Max,
            "health.job_nr_completed_checkpoints",
            Integer,
        ), // does not work w reactive mode
        (
            Jobs,
            "numberOfFailedCheckpoints",
            Max,
            "health.job_nr_failed_checkpoints",
            Integer,
        ), // does not work w reactive mode
        (Task, "numRecordsInPerSecond", Max, MC_FLOW__RECORDS_IN_PER_SEC, Float),
        (Task, "numRecordsOutPerSecond", Max, "flow.records_out_per_sec", Float),
        (TaskManagers, "Status.JVM.CPU.Load", Max, "cluster.task_cpu_load", Float),
        (
            TaskManagers,
            "Status.JVM.Memory.Heap.Used",
            Max,
            "cluster.task_heap_memory_used",
            Float,
        ),
        (
            TaskManagers,
            "Status.JVM.Memory.Heap.Committed",
            Max,
            "cluster.task_heap_memory_committed",
            Float,
        ),
        (
            TaskManagers,
            "Status.JVM.Threads.Count",
            Max,
            "cluster.task_nr_threads",
            Integer,
        ),
        (
            Task,
            "buffers.inputQueueLength",
            Max,
            "cluster.task_network_input_queue_len",
            Float, // Integer,
        ), // verify,
        (
            Task,
            "buffers.inPoolUsage",
            Max,
            "cluster.task_network_input_pool_usage",
            Float, // Integer,
        ), // verify,
        (
            Task,
            "buffers.outputQueueLength",
            Max,
            "cluster.task_network_output_queue_len",
            Float, // Integer,
        ), // verify,
        (
            Task,
            "buffers.outPoolUsage",
            Max,
            "cluster.task_network_output_pool_usage",
            Float, // Integer,
        ), // verify,
    ]
    .into_iter()
    .map(|(scope, m, agg, tp, telemetry_type)| MetricOrder {
        scope,
        metric: m.to_string(),
        agg,
        telemetry_path: tp.to_string(),
        telemetry_type,
    })
    .collect()
});

#[tracing::instrument(level="info", skip(name, scheduler, settings), fields(sensor=%name))]
pub async fn make_sensor(
    name: &str, scheduler: Box<dyn SourceStage<()>>, settings: &FlinkSettings,
) -> Result<Box<dyn SourceStage<Telemetry>>, SenseError> {
    let name: SharedString = format!("{}_flink_sensor", name).into();

    let orders = Arc::new(MetricOrder::extend_standard_with_settings(settings));
    let context = Arc::new(TaskContext::new(settings)?);

    let jobs_scope_sensor = ScopeSensor::new(FlinkScope::Jobs, orders.clone(), context.clone());
    let tm_scope_sensor = ScopeSensor::new(FlinkScope::TaskManagers, orders.clone(), context.clone());
    let tm_admin_sensor = TaskmanagerAdminSensor::new(context.clone());
    let vertex_sensor = VertexSensor::new(orders.clone(), context.clone())?;
    let flink_sensors: Vec<Box<dyn ThroughStage<(), Telemetry>>> = vec![
        Box::new(jobs_scope_sensor),
        Box::new(tm_scope_sensor),
        Box::new(tm_admin_sensor),
        Box::new(vertex_sensor),
    ];

    let broadcast = stage::Broadcast::new(name.clone(), flink_sensors.len());

    let merge_combine = stage::MergeCombine::new(name.clone(), flink_sensors.len());
    let composite_outlet = merge_combine.outlet();

    (scheduler.outlet(), broadcast.inlet()).connect().await;

    let broadcast_outlets = broadcast.outlets();
    let merge_inlets = merge_combine.inlets();
    for (pos, sensor) in flink_sensors.iter().enumerate() {
        (broadcast_outlets.get(pos).unwrap(), &sensor.inlet()).connect().await;

        (sensor.outlet(), merge_inlets.get(pos).await.unwrap()).connect().await;
    }

    let mut composite_graph = Graph::default();
    composite_graph.push_back(scheduler.dyn_upcast()).await;
    composite_graph.push_back(Box::new(broadcast)).await;
    for sensor in flink_sensors {
        composite_graph.push_back(sensor.dyn_upcast()).await;
    }
    composite_graph.push_back(Box::new(merge_combine)).await;

    let composite: Box<dyn SourceStage<Telemetry>> =
        Box::new(stage::CompositeSource::new(name, composite_graph, composite_outlet).await);

    Ok(composite)
}

// fn make_source_scheduler(name: &str, settings: &FlinkSettings) -> stage::Tick<()> {
//     stage::Tick::new(
//         format!("flink_source_{}_tick", name),
//         settings.metrics_initial_delay,
//         settings.metrics_interval,
//         (),
//     )
// }

#[derive(Clone)]
pub struct TaskContext {
    pub client: ClientWithMiddleware,
    pub base_url: Url,
}

impl fmt::Debug for TaskContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskContext").field("base_url", &self.base_url).finish()
    }
}

impl TaskContext {
    pub fn new(settings: &FlinkSettings) -> Result<Self, SenseError> {
        let client = Self::do_make_http_client(settings)?;
        let base_url = settings.base_url()?;
        Ok(Self { client, base_url })
    }

    fn do_make_http_client(settings: &FlinkSettings) -> Result<ClientWithMiddleware, SenseError> {
        let headers = settings.header_map()?;

        let client_builder = reqwest::Client::builder()
            .pool_idle_timeout(settings.pool_idle_timeout)
            .default_headers(headers);

        let client_builder = if let Some(pool_max_idle_per_host) = settings.pool_max_idle_per_host {
            client_builder.pool_max_idle_per_host(pool_max_idle_per_host)
        } else {
            client_builder
        };

        let client = client_builder.build()?;

        let retry_policy = ExponentialBackoff::builder().build_with_max_retries(settings.max_retries);
        Ok(ClientBuilder::new(client)
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build())
    }
}

// This type is also only needed to circumvent the issue cast_trait_object places on forcing the generic Out type.
// Since Telemetry is only used for this stage, The generic variation is dead.
pub trait Unpack: Default + Sized {
    fn unpack(telemetry: Telemetry) -> Result<Self, TelemetryError>;
}

impl Unpack for Telemetry {
    fn unpack(telemetry: Telemetry) -> Result<Self, TelemetryError> {
        Ok(telemetry)
    }
}

pub(crate) static FLINK_SENSOR_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "flink_sensor_time",
            "Time spent collecting telemetry from Flink in seconds",
        )
        .buckets(vec![0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.75, 1.0, 2.5, 5.0, 10.0]),
        &["flink_scope"],
    )
    .expect("failed creating flink_sensor_time metric")
});

pub(crate) static FLINK_SENSOR_ERRORS: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new("flink_sensor_errors", "Number of errors collecting Flink telemetry"),
        &["flink_scope", "error_type"],
    )
    .expect("failed creating flink_sensor_errors metric")
});

#[inline]
pub(crate) fn start_flink_sensor_timer(scope: &FlinkScope) -> HistogramTimer {
    FLINK_SENSOR_TIME
        .with_label_values(&[scope.to_string().as_str()])
        .start_timer()
}

#[inline]
pub(crate) fn track_flink_sensor_errors(scope: &FlinkScope, error: &SenseError) {
    FLINK_SENSOR_ERRORS
        .with_label_values(&[scope.to_string().as_str(), error.label().as_ref()])
        .inc()
}

const JOB_SCOPE: &str = "Jobs";
const TASK_SCOPE: &str = "Tasks";

#[inline]
fn identity_or_track_error<T, E>(scope: FlinkScope, data: Result<T, E>) -> Result<T, SenseError>
where
    E: Into<SenseError>,
{
    match data {
        Ok(data) => Ok(data),
        Err(err) => {
            let error = err.into();
            tracing::error!(%scope, ?error, "failed to collect telemetry from Flink {} sensor", scope);
            track_flink_sensor_errors(&scope, &error);
            Err(error)
        },
    }
}

type OrdersByMetric = HashMap<String, Vec<MetricOrder>>;
/// Distills orders to requested scope and reorganizes them (order has metric+agg) to metric and
/// consolidates aggregation span.
fn distill_metric_orders_and_agg(
    scopes: &HashSet<FlinkScope>, orders: &[MetricOrder],
) -> (OrdersByMetric, HashSet<Aggregation>) {
    let mut order_domain = HashMap::default();
    let mut agg_span = HashSet::default();

    for o in orders.iter().filter(|o| scopes.contains(&o.scope)) {
        agg_span.insert(o.agg);
        let entry = order_domain.entry(o.metric.clone()).or_insert_with(Vec::new);
        entry.push(o.clone());
    }

    (order_domain, agg_span)
}

fn merge_into_metric_groups(metric_telemetry: &mut HashMap<String, Vec<TelemetryValue>>, vertex_telemetry: Telemetry) {
    for (metric, vertex_val) in vertex_telemetry.into_iter() {
        metric_telemetry
            .entry(metric)
            .or_insert_with(Vec::default)
            .push(vertex_val);
    }
}

#[tracing::instrument(level = "info", skip(orders))]
fn consolidate_active_job_telemetry_for_order(
    job_telemetry: HashMap<String, Vec<TelemetryValue>>, orders: &[MetricOrder],
) -> Result<Telemetry, TelemetryError> {
    // to avoid repeated linear searches, reorg strategy data based on metrics
    let mut telemetry_agg = HashMap::with_capacity(orders.len());
    for o in orders {
        telemetry_agg.insert(o.telemetry_path.as_str(), o.agg);
    }

    // merge via order aggregation
    let telemetry: Telemetry = job_telemetry
        .into_iter()
        .map(
            |(metric, values)| match telemetry_agg.get(metric.as_str()).map(|agg| (agg, agg.combinator())) {
                None => Ok(Some((metric, TelemetryValue::Seq(values)))),
                Some((agg, combo)) => {
                    let merger = combo.combine(values.clone()).map(|combined| combined.map(|c| (metric, c)));
                    tracing::info!(?merger, ?values, %agg, "merging metric values per order aggregator");
                    merger
                },
            },
        )
        .collect::<Result<Vec<_>, TelemetryError>>()?
        .into_iter()
        .flatten()
        .collect::<telemetry::TableType>()
        .into();

    Ok(telemetry)
}

#[allow(clippy::cognitive_complexity)]
fn log_response(label: &str, response: &reqwest::Response) {
    const PREAMBLE: &str = "flink telemetry response received";
    let status = response.status();
    if status.is_success() || status.is_informational() {
        tracing::info!(?response, "{PREAMBLE}:{label}");
    } else if status.is_client_error() {
        tracing::error!(?response, "{PREAMBLE}:{label}");
    } else {
        tracing::warn!(?response, "{PREAMBLE}:{label}");
    }
}
