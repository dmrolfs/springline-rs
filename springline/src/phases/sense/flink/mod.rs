use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use cast_trait_object::DynCastExt;
use once_cell::sync::Lazy;
use pretty_snowflake::{AlphabetCodec, IdPrettifier, MachineNode};
use proctor::elements::telemetry::{self, Telemetry, TelemetryValue};
use proctor::error::{SenseError, TelemetryError};
use proctor::graph::stage::{self, SourceStage, ThroughStage};
use proctor::graph::{Connect, Graph, SinkShape, SourceShape, UniformFanInShape, UniformFanOutShape};
use prometheus::{HistogramOpts, HistogramTimer, HistogramVec};

use crate::flink::{self, FlinkContext};
use crate::phases::sense::flink::scope_sensor::ScopeSensor;
use crate::phases::sense::flink::taskmanager_admin_sensor::TaskmanagerAdminSensor;
use crate::phases::sense::flink::vertex_sensor::VertexSensor;
use crate::settings::FlinkSensorSettings;

mod api_model;
mod metric_order;
mod scope_sensor;
mod taskmanager_admin_sensor;
mod vertex_sensor;

pub use metric_order::{Aggregation, FlinkScope, MetricOrder};
pub use vertex_sensor::{
    FLINK_VERTEX_SENSOR_AVAIL_TELEMETRY_TIME, FLINK_VERTEX_SENSOR_METRIC_PICKLIST_TIME, FLINK_VERTEX_SENSOR_TIME,
};

use crate::model::{CorrelationGenerator, MC_FLOW__RECORDS_IN_PER_SEC};

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

#[derive(Debug)]
pub struct FlinkSensorSpecification<'a> {
    pub name: &'a str,
    pub context: FlinkContext,
    pub scheduler: Box<dyn SourceStage<()>>,
    pub settings: &'a FlinkSensorSettings,
    pub machine_node: MachineNode,
}

#[tracing::instrument(level = "trace")]
pub async fn make_sensor(spec: FlinkSensorSpecification<'_>) -> Result<Box<dyn SourceStage<Telemetry>>, SenseError> {
    let name = format!("{}_flink_sensor", spec.name);

    let orders = Arc::new(MetricOrder::extend_standard_with_settings(spec.settings));

    let correlation_gen =
        CorrelationGenerator::distributed(spec.machine_node, IdPrettifier::<AlphabetCodec>::default());
    let jobs_scope_sensor = ScopeSensor::new(
        FlinkScope::Jobs,
        orders.clone(),
        spec.context.clone(),
        correlation_gen.clone(),
    );
    let tm_scope_sensor = ScopeSensor::new(
        FlinkScope::TaskManagers,
        orders.clone(),
        spec.context.clone(),
        correlation_gen.clone(),
    );
    let tm_admin_sensor = TaskmanagerAdminSensor::new(spec.context.clone(), correlation_gen.clone());
    let vertex_sensor = VertexSensor::new(orders.clone(), spec.context.clone(), correlation_gen)?;
    let flink_sensors: Vec<Box<dyn ThroughStage<(), Telemetry>>> = vec![
        Box::new(jobs_scope_sensor),
        Box::new(tm_scope_sensor),
        Box::new(tm_admin_sensor),
        Box::new(vertex_sensor),
    ];

    let broadcast = stage::Broadcast::new(&name, flink_sensors.len());

    let merge_combine = stage::MergeCombine::new(&name, flink_sensors.len());
    let composite_outlet = merge_combine.outlet();

    (spec.scheduler.outlet(), broadcast.inlet()).connect().await;

    let broadcast_outlets = broadcast.outlets();
    let merge_inlets = merge_combine.inlets();
    for (pos, sensor) in flink_sensors.iter().enumerate() {
        (broadcast_outlets.get(pos).unwrap(), &sensor.inlet()).connect().await;

        (sensor.outlet(), merge_inlets.get(pos).await.unwrap()).connect().await;
    }

    let mut composite_graph = Graph::default();
    composite_graph.push_back(spec.scheduler.dyn_upcast()).await;
    composite_graph.push_back(Box::new(broadcast)).await;
    for sensor in flink_sensors {
        composite_graph.push_back(sensor.dyn_upcast()).await;
    }
    composite_graph.push_back(Box::new(merge_combine)).await;

    let composite: Box<dyn SourceStage<Telemetry>> =
        Box::new(stage::CompositeSource::new(&name, composite_graph, composite_outlet).await);

    Ok(composite)
}

// This type is also only needed to circumvent the issue cast_trait_object places on forcing the
// generic Out type. Since Telemetry is only used for this stage, The generic variation is dead.
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

#[inline]
pub(crate) fn start_flink_sensor_timer(scope: &FlinkScope) -> HistogramTimer {
    FLINK_SENSOR_TIME
        .with_label_values(&[scope.to_string().as_str()])
        .start_timer()
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
            flink::track_flink_errors(scope.as_ref(), &error);
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

#[tracing::instrument(level = "trace", skip(orders))]
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
                    tracing::debug!(?merger, ?values, %agg, "merging metric values per order aggregator");
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
