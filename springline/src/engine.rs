mod http;
mod monitor;
mod service;

pub use self::http::run_http_server;
pub use self::monitor::{
    DECISION_PLAN_CURRENT_NR_TASK_MANAGERS, DECISION_SHOULD_PLAN_FOR_SCALING, ELIGIBILITY_IS_ELIGIBLE_FOR_SCALING,
    GOVERNANCE_PLAN_ACCEPTED, PLAN_OBSERVATION_COUNT, PLAN_TARGET_NR_TASK_MANAGERS,
};

use cast_trait_object::DynCastExt;
use enumflags2::{bitflags, BitFlags};
use monitor::Monitor;
use pretty_snowflake::MachineNode;
use proctor::elements::Telemetry;
use proctor::graph::stage::{ActorSourceApi, SinkStage, SourceStage, WithApi, WithMonitor};
use proctor::graph::{Connect, Graph, SinkShape, SourceShape};
use proctor::{ProctorResult, SharedString};
use prometheus::Registry;
use std::fmt::{self, Display};
use std::time::Duration;
use tokio::task::JoinHandle;

use crate::engine::service::{EngineCmd, EngineServiceApi, Health, Service};
use crate::flink::FlinkContext;
use crate::metrics::{self, UpdateMetrics};
use crate::model::MetricCatalog;
use crate::phases::act::ActMonitor;
use crate::phases::governance::{self, GovernanceOutcome};
use crate::phases::{act, decision, eligibility, plan, sense};
use crate::settings::Settings;
use crate::Result;

pub struct Autoscaler;
impl Autoscaler {
    pub fn builder(name: impl Into<SharedString>) -> AutoscaleEngine<Building> {
        AutoscaleEngine::default().with_name(name)
    }
}

#[derive(Debug, Clone)]
pub struct AutoscaleEngine<S: EngineState> {
    pub inner: S,
}

impl Default for AutoscaleEngine<Building> {
    fn default() -> Self {
        Self { inner: Building::default() }
    }
}

pub type PhaseFlags = BitFlags<PhaseFlag>;

#[bitflags(default = Sense | Plan | Act)]
#[repr(u8)]
#[derive(Debug, Display, Copy, Clone, PartialEq)]
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

#[derive(Default)]
pub struct Building {
    name: SharedString,
    sensors: Vec<Box<dyn SourceStage<Telemetry>>>,
    act: Option<Box<dyn SinkStage<GovernanceOutcome>>>,
    rx_action_monitor: Option<act::ActMonitor<GovernanceOutcome>>,
    metrics_registry: Option<&'static Registry>,
    tx_monitor_feedback: Option<ActorSourceApi<Telemetry>>,
}
impl EngineState for Building {}

impl fmt::Debug for Building {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Building")
            .field("name", &self.name)
            .field("nr_custom_sensors", &self.sensors.len())
            .field("metrics_registry", &self.metrics_registry)
            .field("rx_act_monitor", &self.rx_action_monitor)
            .field("tx_monitor_feedback", &self.tx_monitor_feedback)
            .finish()
    }
}

impl AutoscaleEngine<Building> {
    pub fn with_name(self, name: impl Into<SharedString>) -> Self {
        Self {
            inner: Building { name: name.into(), ..self.inner },
        }
    }

    pub fn with_action_stage(self, action_stage: Box<dyn SinkStage<GovernanceOutcome>>) -> Self {
        tracing::trace!(?action_stage, "setting act phase on autoscale engine builder.");
        Self {
            inner: Building { act: Some(action_stage), ..self.inner },
        }
    }

    pub fn with_action_monitor(self, rx_act_monitor: ActMonitor<GovernanceOutcome>) -> Self {
        Self {
            inner: Building {
                rx_action_monitor: Some(rx_act_monitor),
                ..self.inner
            },
        }
    }

    pub fn with_sensors<I>(self, sensors: I) -> Self
    where
        I: Iterator<Item = Box<dyn SourceStage<Telemetry>>>,
    {
        let sensors = sensors.collect();
        tracing::trace!(?sensors, "setting sensors on autoscale engine builder.");
        Self { inner: Building { sensors, ..self.inner } }
    }

    pub fn add_sensor(mut self, sensor: impl SourceStage<Telemetry>) -> Self {
        tracing::trace!(?sensor, "adding sensor to autoscale engine.");
        self.inner.sensors.push(Box::new(sensor));
        self
    }

    pub fn add_monitor_feedback(mut self, tx_monitor_feedback: ActorSourceApi<Telemetry>) -> Self {
        tracing::trace!("adding feedback api for monitor");
        self.inner.tx_monitor_feedback = Some(tx_monitor_feedback);
        self
    }

    pub fn with_metrics_registry(self, registry: &'static Registry) -> Self {
        tracing::trace!(?registry, "added metrics registry to autoscale engine.");
        Self {
            inner: Building { metrics_registry: Some(registry), ..self.inner },
        }
    }

    #[tracing::instrument(level = "trace", skip(self, settings))]
    pub async fn finish(self, flink: FlinkContext, settings: &Settings) -> Result<AutoscaleEngine<Ready>> {
        let machine_node = MachineNode::new(settings.engine.machine_id, settings.engine.node_id)?;

        if let Some(registry) = self.inner.metrics_registry {
            metrics::register_metrics(registry)?;
        }

        let (mut sense_builder, tx_stop_flink_sensor) =
            sense::make_sense_phase("data", flink, &settings.sensor, self.inner.sensors, machine_node).await?;
        let (eligibility_phase, eligibility_channel) =
            eligibility::make_eligibility_phase(&settings.eligibility, &mut sense_builder).await?;
        let rx_eligibility_monitor = eligibility_phase.rx_monitor();

        let (decision_phase, decision_channel) =
            decision::make_decision_phase(&settings.decision, &mut sense_builder).await?;
        let rx_decision_monitor = decision_phase.rx_monitor();

        let planning_phase = plan::make_plan_phase(&settings.plan, &mut sense_builder).await?;
        let rx_plan_monitor = planning_phase.phase.rx_monitor();
        let rx_flink_planning_monitor = planning_phase.rx_flink_planning_monitor;

        let (governance_phase, governance_channel) =
            governance::make_governance_phase(&settings.governance, &mut sense_builder).await?;
        let rx_governance_monitor = governance_phase.rx_monitor();

        let act = self.inner.act.unwrap_or_else(act::make_logger_act_phase);
        let rx_action_monitor = self.inner.rx_action_monitor.unwrap_or_else(|| {
            let (_, rx_dummy) = tokio::sync::broadcast::channel(0);
            rx_dummy
        });

        let sense = sense_builder
            .build_for_out_w_metrics(MetricCatalog::update_metrics_for(
                format!("{}_{}", self.inner.name, "sense").into(),
            ))
            .await?;

        let data_throttle = proctor::graph::stage::ReduceWithin::new(
            "data_throttle",
            Duration::ZERO,
            settings.sensor.flink.metrics_interval,
        );

        let tx_clearinghouse_api = sense.tx_api();

        let service = Service::new(tx_stop_flink_sensor, tx_clearinghouse_api, self.inner.metrics_registry);
        let tx_service_api = service.tx_api();
        let service_handle = tokio::spawn(async move { service.run().await });

        (sense.outlet(), data_throttle.inlet()).connect().await;
        (data_throttle.outlet(), eligibility_phase.inlet()).connect().await;
        (eligibility_phase.outlet(), decision_phase.inlet()).connect().await;
        (decision_phase.outlet(), planning_phase.phase.decision_inlet())
            .connect()
            .await;
        (planning_phase.phase.outlet(), governance_phase.inlet()).connect().await;
        (governance_phase.outlet(), act.inlet()).connect().await;

        let mut graph = Graph::default();
        graph.push_back(Box::new(sense)).await;
        graph.push_back(Box::new(data_throttle)).await;
        graph.push_back(Box::new(eligibility_channel)).await;
        graph.push_back(Box::new(decision_channel)).await;
        graph.push_back(Box::new(planning_phase.data_channel)).await;
        graph.push_back(Box::new(planning_phase.context_channel)).await;
        graph.push_back(Box::new(governance_channel)).await;
        graph.push_back(eligibility_phase).await;
        graph.push_back(decision_phase).await;
        graph.push_back(planning_phase.phase).await;
        graph.push_back(governance_phase).await;
        graph.push_back(act.dyn_upcast()).await;

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
                    tx_feedback: self.inner.tx_monitor_feedback,
                    tx_engine: tx_service_api.clone(),
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
            if let Err(err) = EngineCmd::update_health(&self.inner.tx_service_api, Health::GraphStopped).await {
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
    pub tx_service_api: EngineServiceApi,
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
    #[tracing::instrument(level = "trace")]
    pub async fn block_for_completion(self) -> Result<()> {
        self.inner.graph_handle.await??;
        Ok(())
    }
}
