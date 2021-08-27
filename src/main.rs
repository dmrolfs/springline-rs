use clap::Clap;
use proctor::graph::stage::{SinkStage, SourceStage, ThroughStage};
use proctor::phases::collection::{ClearinghouseApi, SourceSetting, TelemetrySource};
use proctor::tracing::{get_subscriber, init_subscriber};
use springline::phases::decision::DecisionResult;
use springline::phases::eligibility::{make_eligibility_phase, EligibilityOutcome};
use springline::phases::plan::FlinkScalePlan;
use springline::phases::MetricCatalog;
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

#[tracing::instrument(level = "info", skip(_settings))]
async fn make_collection_phase(
    _settings: &Settings,
) -> anyhow::Result<(Box<dyn SourceStage<MetricCatalog>>, ClearinghouseApi)> {
    // let sources = for (ref name, ref source_setting) in settings.collection_sources {
    //     let (telemetry_source, tx_trigger) = match source_setting {
    //         SourceSetting::RestApi(_) => {
    //             let (src, api) = make_telemetry_rest_api_source(name, source_setting).await?;
    //             (src, Some(api))
    //         }
    //
    //         SourceSetting::Csv { path: _ } => {
    //             (make_telemetry_cvs_source(name, source_setting)?, None)
    //         }
    //     };
    // };
    todo!()
}

#[tracing::instrument(level = "info", skip())]
fn make_telemetry_source(
    _source_name: &String,
    _setting: &SourceSetting,
) -> anyhow::Result<TelemetrySource> {
    // match source_name.as_str() {
    //
    // }
    todo!()
}

type DecisionOutcome = DecisionResult<MetricCatalog>;

#[tracing::instrument(level = "info", skip(_settings))]
async fn make_decision_phase(
    _settings: &Settings,
) -> anyhow::Result<Box<dyn ThroughStage<EligibilityOutcome, DecisionOutcome>>> {
    todo!()
}

type PlanningOutcome = FlinkScalePlan;

#[tracing::instrument(level = "info", skip(_settings))]
async fn make_plan_phase(
    _settings: &Settings,
) -> anyhow::Result<Box<dyn ThroughStage<DecisionOutcome, PlanningOutcome>>> {
    todo!()
}

type GoveranceOutcome = PlanningOutcome;

#[tracing::instrument(level = "info", skip(_settings))]
async fn make_governance_phase(
    _settings: &Settings,
) -> anyhow::Result<Box<dyn ThroughStage<PlanningOutcome, GoveranceOutcome>>> {
    todo!()
}

#[tracing::instrument(level = "info", skip(_settings))]
async fn make_execution_phase(
    _settings: &Settings,
) -> anyhow::Result<Box<dyn SinkStage<GoveranceOutcome>>> {
    todo!()
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
