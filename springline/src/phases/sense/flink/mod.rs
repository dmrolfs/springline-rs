use std::collections::HashMap;

use cast_trait_object::DynCastExt;
use once_cell::sync::Lazy;
use pretty_snowflake::{AlphabetCodec, IdPrettifier, MachineNode};
use proctor::elements::telemetry::{self, Telemetry, TelemetryValue};
use proctor::error::{SenseError, TelemetryError};
use proctor::graph::stage::{self, SourceStage, ThroughStage};
use proctor::graph::{Connect, Graph, SinkShape, SourceShape, UniformFanInShape, UniformFanOutShape};
use prometheus::{HistogramOpts, HistogramTimer, HistogramVec};

use crate::flink::{self, CorrelationGenerator, FlinkContext, MC_FLOW__RECORDS_IN_PER_SEC};
use crate::phases::sense::flink::job_taskmanager_sensor::JobTaskmanagerSensor;
use crate::phases::sense::flink::taskmanager_admin_sensor::TaskmanagerAdminSensor;
use crate::phases::sense::flink::vertex_sensor::VertexSensor;
use crate::settings::FlinkSensorSettings;

mod api_model;
mod job_taskmanager_sensor;
mod metric_order;
mod taskmanager_admin_sensor;
mod vertex_sensor;

pub use metric_order::{
    Aggregation, DerivativeCombinator, FlinkScope, MetricOrder, MetricSpec, PlanPositionCandidate, PlanPositionSpec,
};
pub use vertex_sensor::{
    FLINK_VERTEX_SENSOR_AVAIL_TELEMETRY_TIME, FLINK_VERTEX_SENSOR_METRIC_PICKLIST_TIME, FLINK_VERTEX_SENSOR_TIME,
};

// note: `cluster.nr_task_managers` is a standard metric pulled from Flink's admin API. The order
// mechanism may need to be expanded to consider further meta information outside of Flink Metrics
// API.
pub static STD_METRIC_ORDERS: Lazy<Vec<MetricOrder>> = Lazy::new(|| {
    use self::Aggregation::*;
    use proctor::elements::TelemetryType::*;

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
            metric: MetricSpec::new("numRecordsInPerSecond", Max, MC_FLOW__RECORDS_IN_PER_SEC, Float),
        },
        MetricOrder::Task {
            position: PlanPositionSpec::Any,
            metric: MetricSpec::new("numRecordsOutPerSecond", Max, "flow.records_out_per_sec", Float),
        },
        MetricOrder::Task {
            position: PlanPositionSpec::NotSource,
            metric: MetricSpec::new("idleTimeMsPerSecond", Avg, "flow.idle_time_millis_per_sec", Float),
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
            metric: MetricSpec::new("Status.JVM.Threads.Count", Max, "cluster.task_nr_threads", Integer),
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
            telemetry_path: "flow.input_total_lag".to_string(),
            telemetry_type: Integer,
            telemetry_lhs: "flow.input_records_lag_max".to_string(),
            telemetry_rhs: "flow.input_assigned_partitions".to_string(),
            combinator: DerivativeCombinator::Product,
            agg: Sum,
        },
    ]
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

    let orders = MetricOrder::extend_standard_with_settings(spec.settings);

    let correlation_gen =
        CorrelationGenerator::distributed(spec.machine_node, IdPrettifier::<AlphabetCodec>::default());
    let jobs_scope_sensor =
        JobTaskmanagerSensor::new_job_sensor(orders.as_slice(), spec.context.clone(), correlation_gen.clone())?;
    let tm_scope_sensor =
        JobTaskmanagerSensor::new_taskmanager_sensor(orders.as_slice(), spec.context.clone(), correlation_gen.clone())?;
    let tm_admin_sensor = TaskmanagerAdminSensor::new(spec.context.clone(), correlation_gen.clone());
    let vertex_sensor = VertexSensor::new(orders.as_slice(), spec.context.clone(), correlation_gen)?;
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

fn merge_into_metric_groups(metric_telemetry: &mut HashMap<String, Vec<TelemetryValue>>, vertex_telemetry: Telemetry) {
    for (metric, vertex_val) in vertex_telemetry.into_iter() {
        metric_telemetry
            .entry(metric)
            .or_insert_with(Vec::default)
            .push(vertex_val);
    }
}

/// For each set of telemetry values collected for a metric, aggregate them accordingly the desired
/// metric order aggregation.
#[tracing::instrument(level = "trace", skip(orders))]
fn consolidate_active_job_telemetry_for_order(
    job_telemetry: HashMap<String, Vec<TelemetryValue>>, orders: &[MetricOrder],
) -> Result<Telemetry, TelemetryError> {
    // to avoid repeated linear searches, reorg strategy data based on metrics
    let mut telemetry_agg = HashMap::with_capacity(orders.len());
    for o in orders {
        telemetry_agg.insert(o.telemetry().1, o.agg());
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

#[tracing::instrument(level = "trace", skip(telemetry))]
pub fn apply_derivative_orders(mut telemetry: Telemetry, derivative_orders: &[MetricOrder]) -> Telemetry {
    fn extract_terms<'t>(
        t: &'t Telemetry, lhs_path: &'t str, rhs_path: &'t str,
    ) -> Option<(&'t TelemetryValue, &'t TelemetryValue)> {
        let lhs = t.get(lhs_path);
        let rhs = t.get(rhs_path);
        lhs.zip(rhs)
    }

    for order in derivative_orders {
        if let MetricOrder::Derivative {
            telemetry_path,
            telemetry_type,
            telemetry_lhs,
            telemetry_rhs,
            combinator,
            ..
        } = order
        {
            if let Some((lhs, rhs)) = extract_terms(&telemetry, telemetry_lhs, telemetry_rhs) {
                match combinator.combine(lhs, rhs).and_then(|c| telemetry_type.cast_telemetry(c)) {
                    Ok(value) => {
                        let _ = telemetry.insert(telemetry_path.clone(), value);
                    },
                    Err(err) => {
                        tracing::warn!(error=?err, "failed to compute derivative metric order - skipping");
                    },
                }
            }
        }
    }

    telemetry
}

#[cfg(test)]
mod tests {
    use wiremock::{Match, Request};

    pub struct EmptyQueryParamMatcher;
    impl Match for EmptyQueryParamMatcher {
        fn matches(&self, request: &Request) -> bool {
            request.url.query().is_none()
        }
    }

    pub struct QueryParamKeyMatcher(String);

    impl QueryParamKeyMatcher {
        pub fn new(key: impl Into<String>) -> Self {
            Self(key.into())
        }
    }

    impl Match for QueryParamKeyMatcher {
        fn matches(&self, request: &Request) -> bool {
            request.url.query_pairs().any(|q| q.0 == self.0.as_str())
        }
    }
}
