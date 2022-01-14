use once_cell::sync::Lazy;
use proctor::error::{CollectionError, MetricLabel};
use prometheus::{HistogramOpts, HistogramTimer, HistogramVec, IntCounterVec, Opts};
use reqwest_middleware::ClientWithMiddleware;
use std::collections::{HashMap, HashSet};
use std::fmt;
use url::Url;

mod api_model;
mod collect_scope;
#[allow(dead_code)]
mod generators;
mod metric_order;

pub use generators::make_flink_metrics_source;
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
        (Task, "numRecordsInPerSecond", Max, "flow.records_in_per_sec", Float),
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

pub(crate) static FLINK_COLLECTION_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "flink_collection_time",
            "Time spent collecting telemetry from Flink in seconds",
        )
        .buckets(vec![0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 5.0]),
        &["flink_scope"],
    )
    .expect("failed creating flink_collection_time metric")
});

pub(crate) static FLINK_COLLECTION_ERRORS: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new("flink_collection_errors", "Number of errors collecting Flink telemetry"),
        &["flink_scope", "error_type"],
    )
    .expect("failed creating flink_collection_errors metric")
});

#[inline]
pub(crate) fn start_flink_collection_timer(scope: &FlinkScope) -> HistogramTimer {
    FLINK_COLLECTION_TIME
        .with_label_values(&[scope.to_string().as_str()])
        .start_timer()
}

#[inline]
pub(crate) fn track_flink_errors(scope: &FlinkScope, error: &CollectionError) {
    FLINK_COLLECTION_ERRORS
        .with_label_values(&[scope.to_string().as_str(), error.label().as_ref()])
        .inc()
}

#[inline]
fn identity_and_track_errors<T, E>(scope: FlinkScope, data: Result<T, E>) -> Result<T, CollectionError>
where
    E: Into<CollectionError>,
{
    match data {
        Ok(data) => Ok(data),
        Err(err) => {
            let error = err.into();
            tracing::error!(%scope, ?error, "failed to collect Flink {} telemetry part", scope);
            track_flink_errors(&scope, &error);
            Err(error)
        },
    }
}

/// Distills orders to requested scope and reorganizes them (order has metric+agg) to metric and
/// consolidates aggregation span.
fn distill_metric_orders_and_agg(
    scopes: &HashSet<FlinkScope>, orders: &[MetricOrder],
) -> (HashMap<String, Vec<MetricOrder>>, HashSet<Aggregation>) {
    let mut order_domain = HashMap::default();
    let mut agg_span = HashSet::default();

    for o in orders.iter().filter(|o| scopes.contains(&o.scope)) {
        agg_span.insert(o.agg);
        let entry = order_domain.entry(o.metric.clone()).or_insert_with(Vec::new);
        entry.push(o.clone());
    }

    (order_domain, agg_span)
}

fn log_response(label: &str, response: &reqwest::Response) {
    const PREAMBLE: &str = "flink telemetry response received";
    let status = response.status();
    if status.is_success() || status.is_informational() {
        tracing::info!(?response, "{}:{}", PREAMBLE, label);
    } else if status.is_client_error() {
        tracing::error!(?response, "{}:{}", PREAMBLE, label);
    } else {
        tracing::warn!(?response, "{}:{}", PREAMBLE, label);
    }
}
