use std::future::Future;

use clap::Parser;
use once_cell::sync::Lazy;
use proctor::elements::Telemetry;
use proctor::graph::stage::SourceStage;
use proctor::tracing::{get_subscriber, init_subscriber};
use prometheus::Registry;
use settings_loader::SettingsLoader;
use springline::engine::{http, Autoscaler};
use springline::settings::{CliOptions, Settings};
use springline::Result;

static METRICS_REGISTRY: Lazy<Registry> = Lazy::new(|| {
    Registry::new_custom(Some("springline".to_string()), None).expect("failed to create prometheus registry")
});

fn main() -> Result<()> {
    let subscriber = get_subscriber("springline", "info", std::io::stdout);
    init_subscriber(subscriber);

    // console_subscriber::ConsoleLayer::builder()
    // .retention(std::time::Duration::from_secs(60))
    // .init();

    let main_span = tracing::info_span!("main");
    let _main_span_guard = main_span.enter();

    let options = CliOptions::parse();
    let settings = Settings::load(&options)?;


    start_pipeline(async move {
        let engine = Autoscaler::builder("springline")
            .add_source(make_settings_source(&settings))
            .with_metrics_registry(&METRICS_REGISTRY)
            .finish(&settings)
            .await?
            .run();

        http::run_http_server(engine.inner.tx_service_api.clone(), &settings.http).await?;

        engine.block_for_completion().await?;

        Ok(())
    })
}

fn make_settings_source(settings: &Settings) -> impl SourceStage<Telemetry> {
    let mut settings_telemetry: proctor::elements::telemetry::TableType = maplit::hashmap! {
        "min_cluster_size".to_string() => settings.governance.rules.min_cluster_size.into(),
        "max_cluster_size".to_string() => settings.governance.rules.max_cluster_size.into(),
        "min_scaling_step".to_string() => settings.governance.rules.min_scaling_step.into(),
        "max_scaling_step".to_string() => settings.governance.rules.max_scaling_step.into(),
    };
    settings_telemetry.extend(settings.governance.rules.custom.clone());

    // todo: remove with proper eligibility context telemetry - see EligibilityContext
    settings_telemetry.extend(maplit::hashmap! {
        "all_sinks_healthy".to_string() => settings.context_stub.all_sinks_healthy.into(),
        "cluster.is_deploying".to_string() => settings.context_stub.cluster_is_deploying.into(),
        "cluster.last_deployment".to_string() => format!("{}", settings.context_stub.cluster_last_deployment.format(proctor::serde::date::FORMAT)).into(),
    });

    proctor::graph::stage::Sequence::new("settings_source", vec![settings_telemetry.into()])
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
