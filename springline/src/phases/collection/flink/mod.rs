use once_cell::sync::Lazy;

use crate::settings::MetricOrder;

mod generators;
mod model;

// pub use model::{FlinkMetric, FlinkMetricResponse, build_telemetry};
// pub use generators::{TaskContext};

pub static STD_METRIC_ORDERS: Lazy<Vec<MetricOrder>> = Lazy::new(|| {
    use proctor::elements::TelemetryType::*;

    use crate::settings::{Aggregation::*, FlinkScope::*};

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
            Integer,
        ), // verify,
        (
            Task,
            "buffers.inPoolUsage",
            Max,
            "cluster.task_network_input_pool_usage",
            Integer,
        ), // verify,
        (
            Task,
            "buffers.outputQueueLength",
            Max,
            "cluster.task_network_output_queue_len",
            Integer,
        ), // verify,
        (
            Task,
            "buffers.outPoolUsage",
            Max,
            "cluster.task_network_output_pool_usage",
            Integer,
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