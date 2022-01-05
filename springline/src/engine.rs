pub mod http;
pub mod monitor;
mod service;

use cast_trait_object::DynCastExt;
use monitor::Monitor;
use pretty_snowflake::MachineNode;
use proctor::elements::Telemetry;
use proctor::graph::stage::tick::TickApi;
use proctor::graph::stage::{SinkStage, SourceStage, WithApi, WithMonitor};
use proctor::graph::{Connect, Graph, SinkShape, SourceShape};
use proctor::phases::collection::ClearinghouseApi;
use proctor::{ProctorResult, SharedString};
use prometheus::Registry;
use tokio::task::JoinHandle;

use crate::engine::service::{EngineServiceApi, Service};
use crate::phases::governance::{self, GovernanceOutcome};
use crate::phases::{collection, decision, eligibility, execution, plan};
use crate::phases::{MetricCatalog, UpdateMetrics};
use crate::settings::Settings;
use crate::{metrics, Result};

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

/// Represents Autoscaler state.
pub trait EngineState {}

#[derive(Debug, Default)]
pub struct Building {
    name: SharedString,
    sources: Vec<Box<dyn SourceStage<Telemetry>>>,
    execution: Option<Box<dyn SinkStage<GovernanceOutcome>>>,
    metrics_registry: Option<&'static Registry>,
}
impl EngineState for Building {}

#[derive(Debug)]
pub struct Ready {
    // name: SharedString,
    graph: Graph,
    monitor: Monitor,
    tx_stop_flink_source: Option<TickApi>,
    tx_clearinghouse_api: ClearinghouseApi,
    metrics_registry: Option<&'static Registry>,
}
impl EngineState for Ready {}

#[allow(dead_code)]
#[derive(Debug)]
pub struct Running {
    pub tx_service_api: EngineServiceApi,
    graph_handle: JoinHandle<ProctorResult<()>>,
    monitor_handle: JoinHandle<()>,
    service_handle: JoinHandle<()>,
    metrics_registry: Option<&'static Registry>,
}

impl EngineState for Running {}

impl AutoscaleEngine<Building> {
    pub fn with_name(self, name: impl Into<SharedString>) -> Self {
        Self {
            inner: Building { name: name.into(), ..self.inner },
        }
    }

    pub fn with_execution(self, execution_phase: Box<dyn SinkStage<GovernanceOutcome>>) -> Self {
        tracing::info!(?execution_phase, "setting execution phase on autoscale engine builder.");
        Self {
            inner: Building { execution: Some(execution_phase), ..self.inner },
        }
    }

    pub fn with_sources<I>(self, sources: I) -> Self
    where
        I: Iterator<Item = Box<dyn SourceStage<Telemetry>>>,
    {
        let sources = sources.collect();
        tracing::info!(?sources, "setting sources on autoscale engine builder.");
        Self { inner: Building { sources, ..self.inner } }
    }

    pub fn add_source(mut self, source: impl SourceStage<Telemetry>) -> Self {
        tracing::info!(?source, "added source to autoscale engine.");
        self.inner.sources.push(Box::new(source));
        self
    }

    pub fn with_metrics_registry<'a>(self, registry: &'a Registry) -> Self
    where
        'a: 'static,
    {
        tracing::info!(?registry, "added metrics registry to autoscale engine.");
        Self {
            inner: Building { metrics_registry: Some(registry), ..self.inner },
        }
    }

    #[tracing::instrument(level = "info")]
    pub async fn finish(self, settings: &Settings) -> Result<AutoscaleEngine<Ready>> {
        let machine_node = MachineNode::new(settings.engine.machine_id, settings.engine.node_id)?;

        if let Some(registry) = self.inner.metrics_registry {
            metrics::register_metrics(registry)?;
        }

        let (mut collection_builder, tx_stop_flink_source) =
            collection::make_collection_phase("data", &settings.collection, self.inner.sources, machine_node).await?;
        let (eligibility_phase, eligibility_channel) =
            eligibility::make_eligibility_phase(&settings.eligibility, (&mut collection_builder).into()).await?;
        let rx_eligibility_monitor = eligibility_phase.rx_monitor();

        let (decision_phase, decision_channel) =
            decision::make_decision_phase(&settings.decision, (&mut collection_builder).into()).await?;
        let rx_decision_monitor = decision_phase.rx_monitor();

        let (planning_phase, planning_channel) =
            plan::make_plan_phase(&settings.plan, (&mut collection_builder).into()).await?;
        let rx_plan_monitor = planning_phase.rx_monitor();

        let (governance_phase, governance_channel) =
            governance::make_governance_phase(&settings.governance, (&mut collection_builder).into()).await?;
        let rx_governance_monitor = governance_phase.rx_monitor();

        let execution = match self.inner.execution {
            Some(e) => e,
            None => execution::make_execution_phase(&settings.execution).await?,
        };

        let collection = collection_builder
            .build_for_out_w_metrics(MetricCatalog::update_metrics_for(
                format!("{}_{}", self.inner.name, "collection").into(),
            ))
            .await?;

        let tx_clearinghouse_api = collection.tx_api();

        (collection.outlet(), eligibility_phase.inlet()).connect().await;
        (eligibility_phase.outlet(), decision_phase.inlet()).connect().await;
        (decision_phase.outlet(), planning_phase.decision_inlet()).connect().await;
        (planning_phase.outlet(), governance_phase.inlet()).connect().await;
        (governance_phase.outlet(), execution.inlet()).connect().await;

        let mut graph = Graph::default();
        graph.push_back(Box::new(collection)).await;
        graph.push_back(Box::new(eligibility_channel)).await;
        graph.push_back(Box::new(decision_channel)).await;
        graph.push_back(Box::new(planning_channel)).await;
        graph.push_back(Box::new(governance_channel)).await;
        graph.push_back(eligibility_phase).await;
        graph.push_back(decision_phase).await;
        graph.push_back(planning_phase).await;
        graph.push_back(governance_phase).await;
        graph.push_back(execution.dyn_upcast()).await;

        Ok(AutoscaleEngine {
            inner: Ready {
                // name: self.inner.name,
                graph,
                tx_stop_flink_source,
                tx_clearinghouse_api,
                monitor: Monitor::new(
                    rx_eligibility_monitor,
                    rx_decision_monitor,
                    rx_plan_monitor,
                    rx_governance_monitor,
                ),
                metrics_registry: self.inner.metrics_registry,
            },
        })
    }
}

impl AutoscaleEngine<Ready> {
    #[tracing::instrument(level = "info")]
    pub fn run(self) -> AutoscaleEngine<Running> {
        let graph_handle = tokio::spawn(async { self.inner.graph.run().await });

        let monitor_handle = tokio::spawn(async { self.inner.monitor.run().await });

        let service = Service::new(
            self.inner.tx_stop_flink_source,
            self.inner.tx_clearinghouse_api,
            self.inner.metrics_registry,
        );
        let tx_service_api = service.tx_api();
        let service_handle = tokio::spawn(async move { service.run().await });

        AutoscaleEngine {
            inner: Running {
                tx_service_api,
                graph_handle,
                monitor_handle,
                service_handle,
                metrics_registry: self.inner.metrics_registry,
            },
        }
    }
}

impl AutoscaleEngine<Running> {
    #[tracing::instrument(level = "info")]
    pub async fn block_for_completion(self) -> Result<()> {
        self.inner.graph_handle.await??;
        Ok(())
    }
}
