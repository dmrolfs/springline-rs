mod http;
mod monitor;
mod service;

use std::fmt::{self, Display};
use std::sync::Arc;
use std::time::Duration;

use cast_trait_object::DynCastExt;
use enumflags2::{bitflags, BitFlags};
use monitor::Monitor;
use pretty_snowflake::MachineNode;
use proctor::elements::Telemetry;
use proctor::graph::stage::{ActorSourceApi, SinkStage, SourceStage, WithApi, WithMonitor};
use proctor::graph::{Connect, Graph, SinkShape, SourceShape};
use proctor::ProctorResult;
use prometheus::Registry;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

pub use self::http::run_http_server;
pub use self::monitor::{
    DECISION_PLAN_CURRENT_NR_TASK_MANAGERS, DECISION_SHOULD_PLAN_FOR_SCALING, ELIGIBILITY_IS_ELIGIBLE_FOR_SCALING,
    GOVERNANCE_PLAN_ACCEPTED, PLAN_OBSERVATION_COUNT, PLAN_TARGET_NR_TASK_MANAGERS,
};
use crate::engine::service::{EngineCmd, EngineServiceApi, Health, Service};
use crate::flink::FlinkContext;
use crate::metrics::{self, UpdateMetrics};
use crate::model::{MetricCatalog, MetricPortfolio};
use crate::phases::act::ActMonitor;
use crate::phases::decision::DecisionResult;
use crate::phases::governance::{self, GovernanceOutcome};
use crate::phases::{act, decision, eligibility, plan, sense};
use crate::settings::Settings;
use crate::Result;

pub struct Autoscaler;
impl Autoscaler {
    pub fn builder(name: impl Into<String>) -> AutoscaleEngine<Building> {
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

pub type BoxedTelemetrySource = Box<dyn SourceStage<Telemetry>>;
pub type BoxedSourceFactory = Box<dyn Fn(&Settings) -> BoxedTelemetrySource + Send + Sync + 'static>;
pub type FeedbackSource = (BoxedTelemetrySource, ActorSourceApi<Telemetry>);
pub type BoxedFeedbackFactory = Box<dyn Fn(&Settings) -> FeedbackSource + Send + Sync + 'static>;
pub type ActionWithMonitor = (Box<dyn SinkStage<GovernanceOutcome>>, ActMonitor<GovernanceOutcome>);
pub type ActionWithMonitorFactory = Box<dyn Fn(&Settings) -> ActionWithMonitor + Send + Sync + 'static>;

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

    #[tracing::instrument(level = "trace", skip(self, settings))]
    pub async fn finish(self, flink: FlinkContext, settings: &Settings) -> Result<AutoscaleEngine<Ready>> {
        let machine_node = MachineNode::new(settings.engine.machine_id, settings.engine.node_id)?;

        if let Some(registry) = self.inner.metrics_registry {
            metrics::register_metrics(registry)?;
        }

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

        let (mut sense_builder, tx_stop_flink_sensor) =
            sense::make_sense_phase("data", flink, &settings.sensor, sensors, machine_node).await?;
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

        let (act, rx_action_monitor): ActionWithMonitor =
            self.inner.action_factory.as_ref().map(|f| f(settings)).unwrap_or_else(|| {
                let stage = act::make_logger_act_phase();
                let (_, rx) = tokio::sync::broadcast::channel(0);
                (stage, rx)
            });


        // let act = self.inner.act.unwrap_or_else(act::make_logger_act_phase);
        // let rx_action_monitor = self.inner.rx_action_monitor.unwrap_or_else(|| {
        //     let (_, rx_dummy) = tokio::sync::broadcast::channel(0);
        //     rx_dummy
        // });

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

        let collect_portfolio =
            crate::phases::CollectMetricPortfolio::new("collect_portfolio", settings.engine.telemetry_portfolio_window);

        let reduce_portfolio = proctor::graph::stage::Map::new(
            "catalog_from_portfolio",
            |decision_portfolio: DecisionResult<MetricPortfolio>| match decision_portfolio {
                DecisionResult::ScaleUp(portfolio) => DecisionResult::ScaleUp(portfolio.unchecked_head().clone()),
                DecisionResult::ScaleDown(portfolio) => DecisionResult::ScaleDown(portfolio.unchecked_head().clone()),
                DecisionResult::NoAction(portfolio) => DecisionResult::NoAction(portfolio.unchecked_head().clone()),
            },
        );

        let tx_clearinghouse_api = sense.tx_api();

        let service = Service::new(
            tx_stop_flink_sensor,
            tx_clearinghouse_api.clone(),
            self.inner.metrics_registry,
        );
        let tx_service_api = service.tx_api();
        let service_handle = tokio::spawn(async move { service.run().await });

        (sense.outlet(), data_throttle.inlet()).connect().await;
        (data_throttle.outlet(), collect_portfolio.inlet()).connect().await;
        (collect_portfolio.outlet(), eligibility_phase.inlet()).connect().await;
        (eligibility_phase.outlet(), decision_phase.inlet()).connect().await;
        (decision_phase.outlet(), reduce_portfolio.inlet()).connect().await;
        (reduce_portfolio.outlet(), planning_phase.phase.decision_inlet())
            .connect()
            .await;
        (planning_phase.phase.outlet(), governance_phase.inlet()).connect().await;
        (governance_phase.outlet(), act.inlet()).connect().await;

        let mut graph = Graph::default();
        graph.push_back(Box::new(sense)).await;
        graph.push_back(Box::new(data_throttle)).await;
        graph.push_back(Box::new(collect_portfolio)).await;
        graph.push_back(Box::new(eligibility_channel)).await;
        graph.push_back(Box::new(decision_channel)).await;
        graph.push_back(Box::new(reduce_portfolio)).await;
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
                    tx_feedback,
                    tx_engine: tx_service_api.clone(),
                    tx_clearinghouse_api,
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
