use crate::phases::decision::{self, DecisionMonitor};
use crate::phases::eligibility::{self, EligibilityMonitor};
use crate::phases::governance::{self, GovernanceMonitor};
use crate::phases::plan::{self, PlanningStrategy};
use crate::phases::{collection, execution, MetricCatalog, UpdateMetrics};
use crate::settings::Settings;
use crate::Result;
use cast_trait_object::DynCastExt;
use proctor::elements::Telemetry;
use proctor::graph::stage::{self, ActorSourceApi, WithApi, WithMonitor};
use proctor::graph::{Connect, Graph, SinkShape, SourceShape};
use proctor::phases::collection::ClearinghouseApi;
use proctor::phases::plan::PlanMonitor;
use proctor::ProctorResult;
use tokio::task::JoinHandle;

struct Monitor {
    rx_eligibility_monitor: EligibilityMonitor,
    rx_decision_monitor: DecisionMonitor,
    rx_plan_monitor: PlanMonitor<PlanningStrategy>,
    rx_governance_monitor: GovernanceMonitor,
}

//todo: type with Quiescent/Ready status... offer run() in Quiescent; ???
pub struct AutoscaleEngine {
    tx_telemetry_source_api: ActorSourceApi<Telemetry>,
    tx_clearinghouse_api: ClearinghouseApi,
    pub graph_handle: JoinHandle<ProctorResult<()>>,
    monitor: Monitor,
}

impl AutoscaleEngine {
    pub async fn new(settings: Settings) -> Result<Self> {
        let engine_source = stage::ActorSource::new("autoscale_engine_source");
        let tx_telemetry_source_api = engine_source.tx_api();
        let mut collection_builder =
            collection::make_collection_phase(&settings.collection, Some(Box::new(engine_source))).await?;

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

        let execution = execution::make_execution_phase(&settings.execution).await?;

        let collection = collection_builder
            .build_for_out_w_metrics(
                MetricCatalog::update_metrics_for("collection".into()), //todo: expose phase name from CollectBuilder
            )
            .await?;
        let tx_clearinghouse_api = collection.tx_api();

        (collection.outlet(), eligibility.inlet()).connect().await;
        (eligibility.outlet(), decision.inlet()).connect().await;
        (decision.outlet(), plan.decision_inlet()).connect().await;
        (plan.outlet(), governance.inlet()).connect().await;
        (governance.outlet(), execution.inlet()).connect().await;

        let mut g = Graph::default();
        g.push_back(Box::new(collection)).await;
        g.push_back(eligibility).await;
        g.push_back(decision).await;
        g.push_back(plan).await;
        g.push_back(governance).await;
        g.push_back(execution.dyn_upcast()).await;

        let graph_handle = tokio::spawn(async { g.run().await });

        Ok(Self {
            tx_telemetry_source_api,
            tx_clearinghouse_api,
            graph_handle,
            monitor: Monitor {
                rx_eligibility_monitor,
                rx_decision_monitor,
                rx_plan_monitor,
                rx_governance_monitor,
            },
        })
    }
}
