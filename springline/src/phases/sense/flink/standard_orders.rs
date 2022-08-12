use crate::flink::MC_FLOW__RECORDS_IN_PER_SEC;
use crate::phases::sense::flink::Aggregation::*;
use crate::phases::sense::flink::{
    DerivativeCombinator, FlinkScope, MetricOrder, MetricSpec, PlanPositionSpec,
};
use once_cell::sync::Lazy;
use proctor::elements::TelemetryType::*;

// note: `cluster.nr_task_managers` is a standard metric pulled from Flink's admin API. The order
// mechanism may need to be expanded to consider further meta information outside of Flink Metrics
// API.
pub static STD_METRIC_ORDERS: Lazy<Vec<MetricOrder>> = Lazy::new(|| {
    vec![
        MetricOrder::Job {
            metric: MetricSpec::new("uptime", Max, "health.job_uptime_millis", Integer),
        },
        MetricOrder::Job {
            metric: MetricSpec::new("numRestarts", Max, "health.job_nr_restarts", Integer),
        },
        MetricOrder::Job {
            metric: MetricSpec::new(
                "numberOfCompletedCheckpoints",
                Max,
                "health.job_nr_completed_checkpoints",
                Integer,
            ),
        },
        MetricOrder::Job {
            metric: MetricSpec::new(
                "numberOfFailedCheckpoints",
                Max,
                "health.job_nr_failed_checkpoints",
                Integer,
            ),
        },
        MetricOrder::Task {
            position: PlanPositionSpec::Any,
            metric: MetricSpec::new(
                "numRecordsInPerSecond",
                Max,
                MC_FLOW__RECORDS_IN_PER_SEC,
                Float,
            ),
        },
        MetricOrder::Task {
            position: PlanPositionSpec::Source,
            metric: MetricSpec::new(
                "numRecordsOutPerSecond",
                Sum,
                "flow.records_out_per_sec",
                Float,
            ),
        },
        MetricOrder::Task {
            position: PlanPositionSpec::NotSource,
            metric: MetricSpec::new(
                "idleTimeMsPerSecond",
                Avg,
                "flow.idle_time_millis_per_sec",
                Float,
            ),
        },
        MetricOrder::Task {
            position: PlanPositionSpec::Source,
            metric: MetricSpec::new(
                "backPressuredTimeMsPerSecond",
                Avg,
                "flow.source_back_pressured_time_millis_per_sec",
                Float,
            ),
        },
        MetricOrder::TaskManager {
            metric: MetricSpec::new("Status.JVM.CPU.Load", Max, "cluster.task_cpu_load", Float),
        },
        MetricOrder::TaskManager {
            metric: MetricSpec::new(
                "Status.JVM.Memory.Heap.Used",
                Max,
                "cluster.task_heap_memory_used",
                Float,
            ),
        },
        MetricOrder::TaskManager {
            metric: MetricSpec::new(
                "Status.JVM.Memory.Heap.Committed",
                Max,
                "cluster.task_heap_memory_committed",
                Float,
            ),
        },
        MetricOrder::TaskManager {
            metric: MetricSpec::new(
                "Status.JVM.Threads.Count",
                Max,
                "cluster.task_nr_threads",
                Integer,
            ),
        },
        MetricOrder::Task {
            position: PlanPositionSpec::Any,
            metric: MetricSpec::new(
                "buffers.inputQueueLength",
                Max,
                "cluster.task_network_input_queue_len",
                Float,
            ),
        }, // Integer,
        MetricOrder::Task {
            position: PlanPositionSpec::Any,
            metric: MetricSpec::new(
                "buffers.inPoolUsage",
                Max,
                "cluster.task_network_input_pool_usage",
                Float,
            ),
        }, // Integer,
        MetricOrder::Task {
            position: PlanPositionSpec::Any,
            metric: MetricSpec::new(
                "buffers.outputQueueLength",
                Max,
                "cluster.task_network_output_queue_len",
                Float,
            ),
        }, // Integer,
        MetricOrder::Task {
            position: PlanPositionSpec::Any,
            metric: MetricSpec::new(
                "buffers.outPoolUsage",
                Max,
                "cluster.task_network_output_pool_usage",
                Float,
            ),
        }, // Integer,
        MetricOrder::Derivative {
            scope: FlinkScope::Operator,
            position: PlanPositionSpec::Source,
            telemetry_path: "flow.source_total_lag".to_string(),
            telemetry_type: Integer,
            telemetry_lhs: "flow.source_records_lag_max".to_string(),
            telemetry_rhs: "flow.source_assigned_partitions".to_string(),
            combinator: DerivativeCombinator::Product,
            agg: Sum,
        },
    ]
});
