use clap::Clap;
use proctor::elements::{PolicyOutcome, PolicySettings, PolicySubscription, Telemetry};
use proctor::graph::stage::{SinkStage, SourceStage, Stage, ThroughStage};
use proctor::graph::{Connect, Port, SinkShape, SourceShape, ThroughShape};
use proctor::phases::collection;
use proctor::phases::collection::{
    make_telemetry_cvs_source, make_telemetry_rest_api_source, ClearinghouseApi, ClearinghouseCmd,
    SourceSetting, SubscriptionChannel, TelemetrySource, TelemetrySubscription,
};
use proctor::phases::eligibility::Eligibility;
use proctor::tracing::{get_subscriber, init_subscriber};
use proctor::ProctorContext;
use springline::phases::decision::DecisionResult;
use springline::phases::eligibility::context::FlinkEligibilityContext;
use springline::phases::eligibility::policy::EligibilityPolicy;
use springline::phases::plan::FlinkScalePlan;
use springline::phases::MetricCatalog;
use springline::phases::METRIC_CATALOG_REQ_SUBSCRIPTION_FIELDS;
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
        let (collection, tx_clearinghouse_api) = make_collection_phase(&settings).await?;
        let eligibility = make_eligibility_phase(&settings, &tx_clearinghouse_api).await?;
        let decision = make_decision_phase(&settings).await?;
        let plan = make_plan_phase(&settings).await?;
        let governance = make_governance_phase(&settings).await?;
        let execution = make_execution_phase(&settings).await?;
        println!("Hello World! num worker threads:{}", num_cpus::get());
        Ok(())
    })
}

#[tracing::instrument(level = "info", skip(settings))]
async fn make_collection_phase(
    settings: &Settings,
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
    source_name: &String,
    setting: &SourceSetting,
) -> anyhow::Result<TelemetrySource> {
    // match source_name.as_str() {
    //
    // }
    todo!()
}

type EligibilityOutcome = PolicyOutcome<MetricCatalog, FlinkEligibilityContext>;

#[tracing::instrument(level = "info", skip(settings))]
async fn make_eligibility_phase(
    settings: &Settings,
    tx_clearinghouse_api: &ClearinghouseApi,
) -> anyhow::Result<Box<dyn ThroughStage<MetricCatalog, EligibilityOutcome>>> {
    let name = "eligibility";
    let data_channel = do_connect_eligibility_data_channel(name, tx_clearinghouse_api).await?;
    let (policy, context_channel) =
        do_connect_eligibility_context(name, &settings.eligibility_policy, tx_clearinghouse_api)
            .await?;
    let eligibility = Eligibility::new(name, policy);
    (context_channel.outlet(), eligibility.context_inlet())
        .connect()
        .await;
    (data_channel.outlet(), eligibility.inlet()).connect().await;

    let phase: Box<dyn ThroughStage<MetricCatalog, EligibilityOutcome>> = Box::new(eligibility);
    Ok(phase)
}

#[tracing::instrument(level = "info", skip())]
async fn do_connect_eligibility_data_channel(
    channel_name: &str,
    tx_clearinghouse_api: &ClearinghouseApi,
) -> anyhow::Result<SubscriptionChannel<MetricCatalog>> {
    let channel = SubscriptionChannel::new(channel_name).await?;
    let subscription = TelemetrySubscription::new(channel_name)
        .with_required_fields(METRIC_CATALOG_REQ_SUBSCRIPTION_FIELDS.clone());

    let (subscribe_cmd, rx_subscribe_ack) =
        ClearinghouseCmd::subscribe(subscription, channel.subscription_receiver.clone());
    tx_clearinghouse_api.send(subscribe_cmd)?;
    rx_subscribe_ack.await?;
    Ok(channel)
}

#[tracing::instrument(level = "info", skip())]
async fn do_connect_eligibility_context(
    context_name: &str,
    policy_settings: &PolicySettings,
    tx_clearinghouse_api: &ClearinghouseApi,
) -> anyhow::Result<(
    EligibilityPolicy,
    SubscriptionChannel<FlinkEligibilityContext>,
)> {
    let policy = EligibilityPolicy::new(policy_settings);
    let channel = SubscriptionChannel::new(context_name).await?;
    let (subscribe_cmd, rx_subscribe_ack) = ClearinghouseCmd::subscribe(
        policy.subscription(context_name),
        channel.subscription_receiver.clone(),
    );
    tx_clearinghouse_api.send(subscribe_cmd)?;
    rx_subscribe_ack.await?;
    Ok((policy, channel))
}

type DecisionOutcome = DecisionResult<MetricCatalog>;

#[tracing::instrument(level = "info", skip(settings))]
async fn make_decision_phase(
    settings: &Settings,
) -> anyhow::Result<Box<dyn ThroughStage<EligibilityOutcome, DecisionOutcome>>> {
    todo!()
}

type PlanningOutcome = FlinkScalePlan;

#[tracing::instrument(level = "info", skip(settings))]
async fn make_plan_phase(
    settings: &Settings,
) -> anyhow::Result<Box<dyn ThroughStage<DecisionOutcome, PlanningOutcome>>> {
    todo!()
}

type GoveranceOutcome = PlanningOutcome;

#[tracing::instrument(level = "info", skip(settings))]
async fn make_governance_phase(
    settings: &Settings,
) -> anyhow::Result<Box<dyn ThroughStage<PlanningOutcome, GoveranceOutcome>>> {
    todo!()
}

#[tracing::instrument(level = "info", skip(settings))]
async fn make_execution_phase(
    settings: &Settings,
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
