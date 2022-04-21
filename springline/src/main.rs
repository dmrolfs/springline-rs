use std::future::Future;

use clap::Parser;
use once_cell::sync::Lazy;
use proctor::elements::Telemetry;
use proctor::graph::stage::{ActorSourceApi, SourceStage, WithApi, WithMonitor};
use prometheus::Registry;
use settings_loader::SettingsLoader;
use springline::engine::{Autoscaler, BoxedTelemetrySource, FeedbackSource};
use springline::phases::act::ScaleActuator;
use springline::settings::{CliOptions, Settings};
use springline::{engine, Result};
use tracing::Subscriber;

static METRICS_REGISTRY: Lazy<Registry> = Lazy::new(|| {
    Registry::new_custom(Some("springline".to_string()), None).expect("failed to create prometheus registry")
});

fn main() -> Result<()> {
    let subscriber = get_tracing_subscriber("info");
    proctor::tracing::init_subscriber(subscriber);

    let main_span = tracing::trace_span!("main");
    main_span.in_scope(|| {
        let options = CliOptions::parse();
        let settings = Settings::load(&options)?;

        start_pipeline(async move {
            let flink = springline::flink::FlinkContext::from_settings(&settings.flink)?;
            let kube = springline::kubernetes::KubernetesContext::from_settings(&settings).await?;
            let action_flink = flink.clone();
            let action_kube = kube.clone();

            let engine = Autoscaler::builder("springline")
                .add_sensor_factory(make_settings_sensor)
                .add_monitor_feedback_factory(make_monitor_sensor_and_api)
                .with_metrics_registry(&METRICS_REGISTRY)
                .with_action_factory(move |settings| {
                    let actuator = ScaleActuator::new(action_kube.clone(), action_flink.clone(), settings);
                    let rx = actuator.rx_monitor();
                    (Box::new(actuator), rx)
                })
                .finish(flink, &settings)
                .await?
                .run();

            tracing::info!("Starting autoscale management server API...");
            let api_handle = engine::run_http_server(engine.inner.tx_service_api.clone(), &settings.http)?;

            tracing::info!("autoscale engine fully running...");
            engine.block_for_completion().await?;
            api_handle.await??;

            tracing::info!("autoscaling engine stopped.");
            Ok(())
        })
    })
}

fn make_settings_sensor(settings: &Settings) -> BoxedTelemetrySource {
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

    Box::new(proctor::graph::stage::Sequence::new("settings_telemetry", vec![settings_telemetry.into()]))
}

fn make_monitor_sensor_and_api(_settings: &Settings) -> FeedbackSource {
    let src = proctor::graph::stage::ActorSource::new("monitor_sensor");
    let tx_api = src.tx_api();
    (Box::new(src), tx_api)
}

#[tracing::instrument(level="trace", skip(future), fields(worker_threads=num_cpus::get()))]
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

fn get_tracing_subscriber(log_directives: impl AsRef<str>) -> impl Subscriber + Send + Send {
    use tracing_subscriber::layer::SubscriberExt;

    let console = console_subscriber::ConsoleLayer::builder().spawn();
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(log_directives.as_ref()));
    let bunyan_formatting =
        tracing_bunyan_formatter::BunyanFormattingLayer::new("springline".to_string(), std::io::stdout);

    tracing_subscriber::registry::Registry::default()
        .with(console)
        // .with(tracing_subscriber::fmt::layer())
        .with(env_filter)
        .with(tracing_bunyan_formatter::JsonStorageLayer)
        .with(bunyan_formatting)
}
