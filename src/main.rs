use clap::Clap;
use proctor::tracing::{get_subscriber, init_subscriber};
use springline::phases::collection::make_collection_phase;
use springline::phases::decision::make_decision_phase;
use springline::phases::eligibility::make_eligibility_phase;
use springline::phases::execution::make_execution_phase;
use springline::phases::governance::make_governance_phase;
use springline::phases::plan::make_plan_phase;
use springline::settings::{CliOptions, Settings};
use std::future::Future;

fn main() -> anyhow::Result<()> {
    let subscriber = get_subscriber("springline", "trace");
    init_subscriber(subscriber);

    let main_span = tracing::info_span!("main");
    let _main_span_guard = main_span.enter();

    let options = CliOptions::parse();
    let settings = Settings::load(options)?;

    //todo assemble and start pipeline in entry
    start_pipeline(async move {
        let (_collection, tx_clearinghouse_api) = make_collection_phase(&settings).await?;
        let _eligibility = make_eligibility_phase(&settings, &tx_clearinghouse_api).await?;
        let _decision = make_decision_phase(&settings).await?;
        let _plan = make_plan_phase(&settings).await?;
        let _governance = make_governance_phase(&settings).await?;
        let _execution = make_execution_phase(&settings).await?;
        println!("Hello World! num worker threads:{}", num_cpus::get());
        Ok(())
    })
}

#[tracing::instrument(level="info", skip(future), fields(worker_threads=num_cpus::get()))]
fn start_pipeline<F>(future: F) -> anyhow::Result<()>
where
    F: Future<Output = anyhow::Result<()>>,
{
    let worker_threads = num_cpus::get();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()?
        .block_on(future)
}
