use once_cell::sync::Lazy;

mod api_model;
#[allow(dead_code)]
mod generators;
mod metric_order;

pub use generators::make_flink_metrics_source;
pub(crate) use generators::{FLINK_COLLECTION_ERRORS, FLINK_COLLECTION_TIME};
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
