mod http;
mod metrics_exporter;
mod monitor;
mod service;

use crate::engine::service::{EngineCmd, EngineServiceApi, Health, Service};
use crate::flink::{FlinkContext, MetricCatalog};
use crate::metrics::UpdateMetrics;
use crate::phases::act::ActMonitor;
use crate::phases::governance::{self, GovernanceOutcome};
use crate::phases::{act, decision, eligibility, plan, sense};
use crate::settings::Settings;
use crate::Result;
use cast_trait_object::DynCastExt;
use enumflags2::{bitflags, BitFlags};
use futures::{future::FutureExt, pin_mut};
use monitor::Monitor;
use once_cell::sync::Lazy;
use pretty_snowflake::MachineNode;
use proctor::elements::Telemetry;
use proctor::graph::stage::{ActorSourceApi, SinkStage, SourceStage, WithApi, WithMonitor};
use proctor::graph::{Connect, Graph, SinkShape, SourceShape};
use proctor::ProctorResult;
use prometheus::core::{AtomicU64, GenericGauge, GenericGaugeVec};
use prometheus::{Opts, Registry};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use strum_macros::Display;
use sysinfo::SystemExt;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

pub use self::http::{
    run_http_server, shutdown_http_server, HttpJoinHandle, TxHttpGracefulShutdown,
};
pub use self::metrics_exporter::{
    run_metrics_exporter, shutdown_exporter, ExporterJoinHandle, TxExporterGracefulShutdown,
};
pub use self::monitor::{
    DECISION_RESCALE_DECISION, ELIGIBILITY_IS_ELIGIBLE_FOR_SCALING, GOVERNANCE_PLAN_ACCEPTED,
    PLAN_OBSERVATION_COUNT, PLAN_TARGET_NR_TASK_MANAGERS,
};
pub use service::EngineApiError;

pub struct Autoscaler;
impl Autoscaler {
    pub fn builder(name: impl Into<String>) -> AutoscaleEngine<Building> {
        AutoscaleEngine::default().with_name(name)
    }
}

#[derive(Debug, Clone)]
pub struct AutoscaleEngine<S: EngineState> {
    inner: S,
}

impl Default for AutoscaleEngine<Building> {
    fn default() -> Self {
        Self { inner: Building::default() }
    }
}

pub type PhaseFlags = BitFlags<PhaseFlag>;

#[bitflags(default = Sense | Plan | Act)]
#[repr(u8)]
#[derive(Debug, Display, Copy, Clone, PartialEq, Eq)]
pub enum PhaseFlag {
    Sense = 1 << 0,
    Eligibility = 1 << 1,
    Decision = 1 << 2,
    Plan = 1 << 3,
    Governance = 1 << 4,
    Act = 1 << 5,
}

/// Represents Autoscaler state.
pub trait EngineState {}

pub type BoxedTelemetrySource = Box<dyn SourceStage<Telemetry>>;
pub type BoxedSourceFactory =
    Box<dyn Fn(&Settings) -> BoxedTelemetrySource + Send + Sync + 'static>;
pub type FeedbackSource = (BoxedTelemetrySource, ActorSourceApi<Telemetry>);
pub type BoxedFeedbackFactory = Box<dyn Fn(&Settings) -> FeedbackSource + Send + Sync + 'static>;
pub type ActionWithMonitor = (
    Box<dyn SinkStage<GovernanceOutcome>>,
    ActMonitor<GovernanceOutcome>,
);
pub type ActionWithMonitorFactory =
    Box<dyn Fn(&Settings) -> ActionWithMonitor + Send + Sync + 'static>;

#[derive(Default, Clone)]
pub struct Building {
    name: String,
    sensors: Arc<RwLock<Vec<BoxedSourceFactory>>>,
    action_factory: Option<Arc<ActionWithMonitorFactory>>,
    metrics_registry: Option<&'static Registry>,
    feedback_factory: Option<Arc<BoxedFeedbackFactory>>,
}

impl EngineState for Building {}

impl fmt::Debug for Building {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Building")
            .field("name", &self.name)
            .field("metrics_registry", &self.metrics_registry)
            .finish()
    }
}

impl AutoscaleEngine<Building> {
    pub fn with_name(self, name: impl Into<String>) -> Self {
        Self {
            inner: Building { name: name.into(), ..self.inner },
        }
    }

    pub fn with_action_factory<F>(self, action_factory: F) -> Self
    where
        F: Fn(&Settings) -> ActionWithMonitor + Send + Sync + 'static,
    {
        Self {
            inner: Building {
                action_factory: Some(Arc::new(Box::new(action_factory))),
                ..self.inner
            },
        }
    }

    //    pub fn with_action_stage(self, action_stage: Box<dyn SinkStage<GovernanceOutcome>>) -> Self {
    //        tracing::trace!(?action_stage, "setting act phase on autoscale engine builder.");
    //        Self {
    //            inner: Building { act: Some(Arc::new(action_stage), ..self.inner },
    //        }
    //    }
    //
    //    pub fn with_action_monitor(self, rx_act_monitor: ActMonitor<GovernanceOutcome>) -> Self {
    //        Self {
    //            inner: Building {
    //                rx_action_monitor: Some(rx_act_monitor),
    //                ..self.inner
    //            },
    //        }
    //    }
    //
    pub async fn add_sensor_factory<F>(self, sensor_factory: F) -> Self
    where
        F: Fn(&Settings) -> BoxedTelemetrySource + Send + Sync + 'static,
    {
        let mut sensors = self.inner.sensors.write().await;
        sensors.push(Box::new(sensor_factory));
        drop(sensors);
        self
    }

    pub fn add_monitor_feedback_factory<F>(mut self, feedback_factory: F) -> Self
    where
        F: Fn(&Settings) -> FeedbackSource + Send + Sync + 'static,
    {
        self.inner.feedback_factory = Some(Arc::new(Box::new(feedback_factory)));
        self
    }

    pub fn with_metrics_registry(self, registry: &'static Registry) -> Self {
        tracing::trace!(?registry, "added metrics registry to autoscale engine.");
        Self {
            inner: Building { metrics_registry: Some(registry), ..self.inner },
        }
    }

    fn tap_system() -> sysinfo::System {
        sysinfo::System::new_with_specifics(sysinfo::RefreshKind::new().with_memory())
    }

    #[tracing::instrument(level = "trace", skip(self, settings))]
    pub async fn finish(
        self, sensor_flink: FlinkContext, settings: &Settings,
    ) -> Result<AutoscaleEngine<Ready>> {
        let machine_node = MachineNode::new(settings.engine.machine_id, settings.engine.node_id)?;

        let system = Self::tap_system();

        let mut sensors = self
            .inner
            .sensors
            .read()
            .await
            .iter()
            .map(|s| s(settings))
            .collect::<Vec<_>>();

        let tx_feedback = self.inner.feedback_factory.as_ref().map(|f| {
            let (source, tx) = f(settings);
            sensors.push(source);
            tx
        });

        let (mut sense_builder, tx_stop_flink_sensor) = sense::make_sense_phase(
            "data",
            sensor_flink,
            &settings.sensor,
            sensors,
            machine_node,
        )
        .await?;
        let (eligibility_phase, eligibility_channel) =
            eligibility::make_eligibility_phase(&settings.eligibility, &mut sense_builder).await?;
        let rx_eligibility_monitor = eligibility_phase.rx_monitor();

        let (decision_phase, decision_channel) =
            decision::make_decision_phase(&settings.decision, &mut sense_builder).await?;
        let rx_decision_monitor = decision_phase.rx_monitor();

        let planning_phase = plan::make_plan_phase(settings, &mut sense_builder).await?;
        let rx_plan_monitor = planning_phase.phase.rx_monitor();
        let rx_flink_planning_monitor = planning_phase.rx_flink_planning_monitor;

        let (governance_phase, governance_channel) =
            governance::make_governance_phase(&settings.governance, &mut sense_builder).await?;
        let rx_governance_monitor = governance_phase.rx_monitor();

        let (act, rx_action_monitor): ActionWithMonitor =
            self.inner.action_factory.as_ref().map(|f| f(settings)).unwrap_or_else(|| {
                let stage = act::make_logger_act_phase();
                let (_, rx) = tokio::sync::broadcast::channel(0);
                (stage, rx)
            });

        let sense = sense_builder
            .build_for_out_w_metrics(MetricCatalog::update_metrics_for(&format!(
                "{}_{}",
                self.inner.name, "sense"
            )))
            .await?;

        let data_throttle = proctor::graph::stage::ReduceWithin::new(
            "data_throttle",
            Duration::ZERO,
            settings.sensor.flink.metrics_interval,
        );

        let evaluation_duration = settings.decision.template_data.as_ref().and_then(|td| {
            td.evaluate_duration_secs.map(|secs| Duration::from_secs(u64::from(secs)))
        });

        let collect_window = crate::phases::CollectMetricWindow::new(
            "collect_window",
            evaluation_duration,
            &settings.engine,
            Some(system),
        );
        let tx_collect_window_api = collect_window.tx_api();

        //todo: maybe place this between governance and action?
        // let _reduce_window = proctor::graph::stage::Map::new(
        //     "catalog_from_window",
        //     |decision_window: DecisionOutcome| {
        //         decision_window.map(|window| match &window {
        //             DecisionResult::ScaleUp(p) => DecisionResult::ScaleUp(p.latest().clone()),
        //             DecisionResult::ScaleDown(p) => DecisionResult::ScaleDown(p.latest().clone()),
        //             DecisionResult::NoAction(p) => DecisionResult::NoAction(p.latest().clone()),
        //         })
        //     },
        // );
        // let reduce_window = reduce_window.with_block_logging();

        let tx_clearinghouse_api = sense.tx_api();

        let service = Service::new(
            tx_stop_flink_sensor,
            tx_clearinghouse_api.clone(),
            self.inner.metrics_registry,
            settings,
        );
        let tx_service_api = service.tx_api();
        let service_handle = tokio::spawn(async move { service.run().await });

        (sense.outlet(), data_throttle.inlet()).connect().await;
        (data_throttle.outlet(), collect_window.inlet()).connect().await;
        (collect_window.outlet(), eligibility_phase.inlet()).connect().await;
        (eligibility_phase.outlet(), decision_phase.inlet()).connect().await;

        // (decision_phase.outlet(), reduce_window.inlet()).connect().await;
        // (reduce_window.outlet(), planning_phase.phase.decision_inlet()).connect().await;

        (
            decision_phase.outlet(),
            planning_phase.phase.decision_inlet(),
        )
            .connect()
            .await;

        (planning_phase.phase.outlet(), governance_phase.inlet()).connect().await;
        (governance_phase.outlet(), act.inlet()).connect().await;

        let mut graph = Graph::default();
        graph.push_back(Box::new(sense)).await;
        graph.push_back(Box::new(data_throttle)).await;
        graph.push_back(Box::new(collect_window)).await;
        graph.push_back(Box::new(eligibility_channel)).await;
        graph.push_back(Box::new(decision_channel)).await;
        // graph.push_back(Box::new(reduce_window)).await;
        graph.push_back(Box::new(planning_phase.data_channel)).await;
        graph.push_back(Box::new(planning_phase.context_channel)).await;
        graph.push_back(Box::new(governance_channel)).await;
        graph.push_back(eligibility_phase).await;
        graph.push_back(decision_phase).await;
        graph.push_back(planning_phase.phase).await;
        graph.push_back(governance_phase).await;
        graph.push_back(act.dyn_upcast()).await;

        let decision_window = settings
            .decision
            .template_data
            .as_ref()
            .and_then(|td| td.evaluate_duration_secs)
            .unwrap_or(60);

        Ok(AutoscaleEngine {
            inner: Ready {
                graph,
                monitor: Monitor {
                    rx_eligibility_monitor,
                    rx_decision_monitor,
                    rx_plan_monitor,
                    rx_flink_planning_monitor,
                    rx_governance_monitor,
                    rx_action_monitor,
                    tx_feedback,
                    tx_engine: tx_service_api.clone(),
                    tx_clearinghouse_api,
                    tx_collect_window_api,
                    decision_window,
                },
                metrics_registry: self.inner.metrics_registry,
                service_handle,
                tx_service_api,
            },
        })
    }
}

pub struct Ready {
    graph: Graph,
    monitor: Monitor,
    metrics_registry: Option<&'static Registry>,
    service_handle: JoinHandle<()>,
    tx_service_api: EngineServiceApi,
}

impl EngineState for Ready {}

impl fmt::Debug for Ready {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Ready")
    }
}

impl AutoscaleEngine<Ready> {
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn run(self) -> AutoscaleEngine<Running> {
        let tx_service_api = self.inner.tx_service_api.clone();
        let graph_handle = tokio::spawn(async move {
            let result = self.inner.graph.run().await;
            if let Err(err) =
                EngineCmd::update_health(&self.inner.tx_service_api, Health::GraphStopped).await
            {
                tracing::error!(error=?err, "Graph stopped but failed to notify engine of health status.");
            }
            result
        });
        let monitor_handle = tokio::spawn(async { self.inner.monitor.run().await });

        AutoscaleEngine {
            inner: Running {
                tx_service_api,
                graph_handle,
                monitor_handle,
                service_handle: self.inner.service_handle,
                metrics_registry: self.inner.metrics_registry,
            },
        }
    }
}

#[allow(dead_code)]
pub struct Running {
    tx_service_api: EngineServiceApi,
    graph_handle: JoinHandle<ProctorResult<()>>,
    monitor_handle: JoinHandle<()>,
    service_handle: JoinHandle<()>,
    metrics_registry: Option<&'static Registry>,
}

impl EngineState for Running {}

impl fmt::Debug for Running {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Running")
    }
}

impl AutoscaleEngine<Running> {
    pub fn tx_service_api(&self) -> EngineServiceApi {
        self.inner.tx_service_api.clone()
    }

    #[tracing::instrument(level = "trace")]
    pub async fn block_for_completion(self) -> Result<bool> {
        let graph_handle = self.inner.graph_handle.fuse();
        let monitor_handle = self.inner.monitor_handle.fuse();
        let service_handle = self.inner.service_handle.fuse();

        pin_mut!(graph_handle, monitor_handle, service_handle);

        futures::select! {
            graph_result = graph_handle => {
                match graph_result {
                    Ok(Ok(())) => {
                        tracing::info!("Graph completed.");
                        Ok(false)
                    },
                    Ok(Err(err)) => {
                        tracing::error!(error=?err, "Graph completed with error.");
                        Err(err.into())
                    },
                    Err(err) => {
                        tracing::error!(error=?err, "Graph failed to complete.");
                        Err(err.into())
                    },
                }
            },
            monitor_result = monitor_handle => {
                match monitor_result {
                    Ok(()) => {
                        tracing::info!("Monitor completed.");
                        Ok(true)
                    },
                    Err(err) => {
                        tracing::error!(error=?err, "Monitor failed to complete.");
                        Err(err.into())
                    },
                }
            },
            service_result = service_handle => {
                match service_result {
                    Ok(()) => {
                        tracing::info!("Service completed.");
                        Ok(true)
                    },
                    Err(err) => {
                        tracing::error!(error=?err, "Service failed to complete.");
                        Err(err.into())
                    },
                }
            },
        }
    }
}

pub static ENGINE_PROCESS_MEMORY: Lazy<GenericGaugeVec<AtomicU64>> = Lazy::new(|| {
    GenericGaugeVec::new(
        Opts::new("engine_process_memory", "The process memory")
            .const_labels(proctor::metrics::CONST_LABELS.clone()),
        &["pid", "name"],
    )
    .expect("failed creating engine_process_memory metric")
});

pub static ENGINE_SYSTEM_TOTAL_MEMORY: Lazy<GenericGauge<AtomicU64>> = Lazy::new(|| {
    GenericGauge::with_opts(
        Opts::new(
            "engine_system_total_memory",
            "The amount of total RAM in bytes for the system",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating engine_system_total_memory metric")
});

pub static ENGINE_SYSTEM_FREE_MEMORY: Lazy<GenericGauge<AtomicU64>> = Lazy::new(|| {
    GenericGauge::with_opts(
        Opts::new(
            "engine_system_free_memory",
            r##"The amount of free RAM in bytes for the system. Generally, "free" memory refers to unallocated memory whereas "available" memory refers to memory that is available for (re)use."##,
        )
            .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
        .expect("failed creating engine_system_free_memory metric")
});

pub static ENGINE_SYSTEM_AVAILABLE_MEMORY: Lazy<GenericGauge<AtomicU64>> = Lazy::new(|| {
    GenericGauge::with_opts(
        Opts::new(
            "engine_system_available_memory",
            r##"The amount of available RAM in bytes for the system. Generally, "free" memory refers to unallocated memory whereas "available" memory refers to memory that is available for (re)use."##,
        )
            .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
        .expect("failed creating engine_system_available_memory metric")
});

pub static ENGINE_SYSTEM_USED_MEMORY: Lazy<GenericGauge<AtomicU64>> = Lazy::new(|| {
    GenericGauge::with_opts(
        Opts::new(
            "engine_system_used_memory",
            "The amount of used RAM in bytes for the system",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating engine_system_used_memory metric")
});

pub static ENGINE_SYSTEM_TOTAL_SWAP: Lazy<GenericGauge<AtomicU64>> = Lazy::new(|| {
    GenericGauge::with_opts(
        Opts::new(
            "engine_system_total_swap",
            "The SWAP size in bytes for the system",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating engine_system_total_swap metric")
});

pub static ENGINE_SYSTEM_FREE_SWAP: Lazy<GenericGauge<AtomicU64>> = Lazy::new(|| {
    GenericGauge::with_opts(
        Opts::new(
            "engine_system_free_swap",
            "The amount of free SWAP in bytes for the system",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating engine_system_free_swap metric")
});

pub static ENGINE_SYSTEM_USED_SWAP: Lazy<GenericGauge<AtomicU64>> = Lazy::new(|| {
    GenericGauge::with_opts(
        Opts::new(
            "engine_system_used_swap",
            "The amount of used SWAP in bytes for the system",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating engine_system_used_swap metric")
});
