use cast_trait_object::DynCastExt;
use clap::Clap;
use proctor::graph::stage::WithMonitor;
use proctor::graph::{Connect, Graph, SinkShape, SourceShape};
use proctor::tracing::{get_subscriber, init_subscriber};
use springline::phases::collection::make_collection_phase;
use springline::phases::decision::make_decision_phase;
use springline::phases::eligibility::make_eligibility_phase;
use springline::phases::execution::make_execution_phase;
use springline::phases::governance::make_governance_phase;
use springline::phases::plan::make_plan_phase;
use springline::settings::{CliOptions, Settings};
use springline::Result;
use std::future::Future;

fn main() -> Result<()> {
    let subscriber = get_subscriber("springline", "trace");
    init_subscriber(subscriber);

    let main_span = tracing::info_span!("main");
    let _main_span_guard = main_span.enter();

    let options = CliOptions::parse();
    let settings = Settings::load(options)?;

    //todo assemble and start pipeline in entry
    start_pipeline(async move {
        let (collection, tx_clearinghouse_api) = make_collection_phase(&settings).await?;

        let eligibility = make_eligibility_phase(&settings, &tx_clearinghouse_api).await?;
        let _eligibility_monitor = eligibility.rx_monitor();

        let decision = make_decision_phase(&settings, &tx_clearinghouse_api).await?;
        let _decision_monitor = decision.rx_monitor();

        let plan = make_plan_phase(&settings, &tx_clearinghouse_api).await?;
        let _plan_monitor = plan.rx_monitor();

        let governance = make_governance_phase(&settings, &tx_clearinghouse_api).await?;
        let _governance_monitor = governance.rx_monitor();

        let execution = make_execution_phase(&settings).await?;
        println!("Hello World! num worker threads:{}", num_cpus::get());

        (collection.outlet(), eligibility.inlet()).connect().await;
        (eligibility.outlet(), decision.inlet()).connect().await;
        (decision.outlet(), plan.decision_inlet()).connect().await;
        (plan.outlet(), governance.inlet()).connect().await;
        (governance.outlet(), execution.inlet()).connect().await;

        let mut g = Graph::default();
        g.push_back(collection.dyn_upcast()).await;
        g.push_back(eligibility).await;
        g.push_back(decision).await;
        g.push_back(plan).await;
        g.push_back(governance).await;
        g.push_back(execution.dyn_upcast()).await;

        g.run().await?;

        Ok(())
    })
}

#[tracing::instrument(level="info", skip(future), fields(worker_threads=num_cpus::get()))]
fn start_pipeline<F>(future: F) -> Result<()>
where
    F: Future<Output = Result<()>>,
{
    let worker_threads = num_cpus::get();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()?
        .block_on(future)
}
