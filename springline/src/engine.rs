mod http;
pub mod monitor;
mod service;

use cast_trait_object::DynCastExt;
use monitor::Monitor;
use pretty_snowflake::MachineNode;
use proctor::elements::Telemetry;
use proctor::graph::stage::{SinkStage, SourceStage, WithApi, WithMonitor};
use proctor::graph::{Connect, Graph, SinkShape, SourceShape};
use proctor::phases::collection::ClearinghouseApi;
use proctor::{ProctorResult, SharedString};
use prometheus::Registry;
use tokio::task::JoinHandle;

use crate::phases::governance::{self, GovernanceOutcome};
use crate::phases::{collection, decision, eligibility, execution, plan};
use crate::phases::{MetricCatalog, UpdateMetrics};
use crate::settings::Settings;
use crate::Result;

pub struct Autoscaler;
impl Autoscaler {
    pub fn builder<'r>(name: impl Into<SharedString>) -> AutoscaleEngine<Building<'r>> {
        AutoscaleEngine::default().with_name(name)
    }
}

#[derive(Debug, Clone)]
pub struct AutoscaleEngine<S: EngineState> {
    inner: S,
}

impl<'r> Default for AutoscaleEngine<Building<'r>> {
    fn default() -> Self {
        Self { inner: Building::default() }
    }
}

/// Represents Autoscaler state.
pub trait EngineState {}

#[derive(Debug, Default)]
pub struct Building<'r> {
    name: SharedString,
    sources: Vec<Box<dyn SourceStage<Telemetry>>>,
    execution: Option<Box<dyn SinkStage<GovernanceOutcome>>>,
    metrics_registry: Option<&'r Registry>,
}
impl<'r> EngineState for Building<'r> {}

#[derive(Debug)]
pub struct Ready<'r> {
    name: SharedString,
    graph: Graph,
    monitor: Monitor,
    tx_clearinghouse_api: ClearinghouseApi,
    metrics_registry: Option<&'r Registry>,
}
impl<'r> EngineState for Ready<'r> {}

#[derive(Debug)]
pub struct Running<'r> {
    name: SharedString,
    graph_handle: JoinHandle<ProctorResult<()>>,
    monitor_handle: JoinHandle<()>,
    tx_clearinghouse_api: ClearinghouseApi,
    metrics_registry: Option<&'r Registry>,
}
impl<'r> EngineState for Running<'r> {}

impl<'r> AutoscaleEngine<Building<'r>> {
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
        'a: 'r,
    {
        tracing::info!(?registry, "added metrics registry to autoscale engine.");
        Self {
            inner: Building { metrics_registry: Some(registry), ..self.inner },
        }
    }

    #[tracing::instrument(level = "info")]
    pub async fn finish(self, settings: Settings) -> Result<AutoscaleEngine<Ready<'r>>> {
        let machine_node = MachineNode::new(settings.engine.machine_id, settings.engine.node_id)?;

        if let Some(registry) = self.inner.metrics_registry {
            crate::metrics::register_metrics(registry)?;
        }

        let mut collection_builder =
            collection::make_collection_phase(&settings.collection, self.inner.sources, machine_node).await?;

        let eligibility =
            eligibility::make_eligibility_phase(&settings.eligibility, (&mut collection_builder).into()).await?;
        let rx_eligibility_monitor = eligibility.rx_monitor();

        let decision = decision::make_decision_phase(&settings.decision, (&mut collection_builder).into()).await?;
        let rx_decision_monitor = decision.rx_monitor();

        let plan = plan::make_plan_phase(&settings.plan, (&mut collection_builder).into()).await?;
        let rx_plan_monitor = plan.rx_monitor();

        let governance =
            governance::make_governance_phase(&settings.governance, (&mut collection_builder).into()).await?;
        let rx_governance_monitor = governance.rx_monitor();

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

        (collection.outlet(), eligibility.inlet()).connect().await;
        (eligibility.outlet(), decision.inlet()).connect().await;
        (decision.outlet(), plan.decision_inlet()).connect().await;
        (plan.outlet(), governance.inlet()).connect().await;
        (governance.outlet(), execution.inlet()).connect().await;

        let mut graph = Graph::default();
        graph.push_back(Box::new(collection)).await;
        graph.push_back(eligibility).await;
        graph.push_back(decision).await;
        graph.push_back(plan).await;
        graph.push_back(governance).await;
        graph.push_back(execution.dyn_upcast()).await;

        Ok(AutoscaleEngine {
            inner: Ready {
                name: self.inner.name,
                graph,
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

impl<'r> AutoscaleEngine<Ready<'r>> {
    #[tracing::instrument(level = "info")]
    pub fn run(self) -> AutoscaleEngine<Running<'r>> {
        let graph = self.inner.graph;
        let monitor = self.inner.monitor;
        let graph_handle = tokio::spawn(async { graph.run().await });
        let monitor_handle = tokio::spawn(async { monitor.run().await });
        AutoscaleEngine {
            inner: Running {
                name: self.inner.name,
                graph_handle,
                monitor_handle,
                tx_clearinghouse_api: self.inner.tx_clearinghouse_api,
                metrics_registry: self.inner.metrics_registry,
            },
        }
    }
}

impl<'r> AutoscaleEngine<Running<'r>> {
    #[tracing::instrument(level = "info")]
    pub async fn block_for_completion(self) -> Result<()> {
        self.inner.graph_handle.await??;
        Ok(())
    }
}
