use std::future::Future;

use clap::Parser;
use futures::{future::FutureExt, pin_mut};
use once_cell::sync::Lazy;
use proctor::elements::telemetry::TableType;
use proctor::graph::stage::{WithApi, WithMonitor};
use prometheus::Registry;
use settings_loader::{LoadingOptions, SettingsLoader};
use springline::engine::{Autoscaler, BoxedTelemetrySource, FeedbackSource};
use springline::flink::FlinkContext;
use springline::kubernetes::KubernetesContext;
use springline::phases::act::ScaleActuator;
use springline::phases::plan::{
    PLANNING__MAX_CATCH_UP, PLANNING__RECOVERY_VALID, PLANNING__RESCALE_RESTART,
};
use springline::settings::{CliOptions, FlinkSettings, Settings};
use springline::{engine, math, Result};
use tokio::signal::unix::{self, SignalKind};
use tracing::Subscriber;

static METRICS_REGISTRY: Lazy<Registry> = Lazy::new(|| {
    Registry::new_custom(Some("springline".to_string()), None)
        .expect("failed to create prometheus registry")
});

fn main() -> Result<()> {
    let subscriber = get_tracing_subscriber("info");
    proctor::tracing::init_subscriber(subscriber);

    let mut restarts_remaining = 3;
    let app_environment = std::env::var(CliOptions::env_app_environment())?;
    let options = CliOptions::parse();

    let main_span = tracing::trace_span!("main");
    let outcome = main_span.in_scope(|| {
        springline::metrics::register_metrics(&METRICS_REGISTRY)?;

        start_pipeline(async move {
            while 0 <= restarts_remaining {
                let settings = Settings::load(&options)?;
                tracing::info!(?options, ?settings, %app_environment, "loaded settings via CLI options");

                // set up flink clients used in sense and act phases
                let sensor_flink = FlinkContext::from_settings("sensor", &FlinkSettings { max_retries: 0, ..settings.flink.clone() } )?;
                sensor_flink.check().await?;
                let action_flink = FlinkContext::from_settings("action", &settings.flink)?;
                action_flink.check().await?;

                // set up k8s client used in act phase
                let kube = KubernetesContext::from_settings("action", &settings).await?;
                kube.check().await?;
                let action_kube = kube; //todo will clone once kube incorporated in sensing

                // build core springline autoscale engine
                let engine_builder = Autoscaler::builder("springline")
                    .add_sensor_factory(make_settings_sensor)
                    .await
                    .add_monitor_feedback_factory(make_monitor_sensor_and_api)
                    .with_metrics_registry(&METRICS_REGISTRY)
                    .with_action_factory(move |settings| {
                        let actuator = ScaleActuator::new(action_kube.clone(), action_flink.clone(), settings);
                        let rx = actuator.rx_monitor();
                        (Box::new(actuator), rx)
                    });


                tracing::info!("Starting autoscale engine...");
                let engine = engine_builder.clone().finish(sensor_flink, &settings).await?.run();
                let tx_service_api = engine.tx_service_api();
                let engine_handle = engine.block_for_completion().fuse();
                tracing::info!("autoscale engine running...");

                tracing::info!("Starting autoscale metrics exporter...");
                let (exporter_handle, tx_shutdown_exporter) = engine::run_metrics_exporter(tx_service_api.clone(), &settings)?;
                let exporter_handle = exporter_handle.fuse();
                tracing::info!("autoscale metrics exporter listening on port {}...", settings.prometheus.port);

                tracing::info!("Starting autoscale management server API...");
                let (api_handle, tx_shutdown_http) = engine::run_http_server(tx_service_api, &settings.http)?;
                let api_handle = api_handle.fuse();
                tracing::info!("autoscale management server API running...");

                let ctrlc_signal = tokio::signal::ctrl_c().fuse();
                let mut terminate_signal = unix::signal(SignalKind::terminate())?;

                pin_mut!(engine_handle, exporter_handle, api_handle, );

                futures::select! {
                    engine_result = engine_handle => {
                        match engine_result {
                            Ok(true) => {
                                tracing::warn!("restarting Autoscale engine...");
                                restarts_remaining += 1;
                            },
                            Ok(false) => {
                                tracing::info!("Autoscale engine stopped");
                                break;
                            },
                            Err(err) => {
                                tracing::error!(%restarts_remaining, "Autoscale engine stopped with error: {}", err);
                            },
                        }
                    },

                    api_result = api_handle => {
                        match api_result {
                            Ok(Ok(())) => {
                                tracing::warn!("Autoscale management server API stopped");
                                break;
                            },
                            Ok(Err(err)) => {
                                tracing::error!(%restarts_remaining, "Autoscale management server API completed with error: {}", err);
                            },
                            Err(err) => {
                                tracing::error!(%restarts_remaining, "Autoscale management server API failed with error: {}", err);
                            },
                        }
                    },

                    exporter_result = exporter_handle => {
                        match exporter_result {
                            Ok(Ok(())) => {
                                tracing::warn!("Autoscale metrics exporter stopped");
                                break;
                            },
                            Ok(Err(err)) => {
                                tracing::error!(%restarts_remaining, "Autoscale metrics exporter completed with error: {}", err);
                            },
                            Err(err) => {
                                tracing::error!(%restarts_remaining, "Autoscale metrics exporter failed with error: {}", err);
                            },
                        }
                    },

                    ctrlc = ctrlc_signal.fuse() => {
                        match ctrlc {
                            Ok(()) => {
                                tracing::info!("springline terminating on user request.");
                                break;
                            },
                            Err(err) => {
                                tracing::warn!(error=?err, "error in signal handling");
                            },
                        }
                    },

                    terminate = terminate_signal.recv().fuse() => {
                        if terminate.is_some() {
                            tracing::warn!("terminate signal received - terminating springline process.");
                            break;
                        }
                    },
                }

                tracing::info!("shutting down Autoscale engine API prior to restart...");
                if let Err(err) = engine::shutdown_http_server(tx_shutdown_http) {
                    if 0 < restarts_remaining {
                        tracing::error!(
                            %restarts_remaining, error=?err,
                            "failed to send shutdown signal to Autoscale engine API -- attempting restart but API may not be accessible."
                        );
                    } else {
                        tracing::error!(%restarts_remaining, error=?err, "failed to send shutdown signal to Autoscale engine API - no restarts left.");
                    }
                }

                tracing::info!("shutting down metrics exporter prior to restart...");
                if let Err(err) = engine::shutdown_exporter(tx_shutdown_exporter) {
                    if 0 < restarts_remaining {
                        tracing::error!(
                            %restarts_remaining, error=?err,
                            "failed to send shutdown signal to Autoscale metrics exporter -- attempting restart but exporter may not be accessible."
                        );
                    } else {
                        tracing::error!(%restarts_remaining, error=?err, "failed to send shutdown signal to Autoscale metrics exporter - no restarts left.");
                    }
                }

                tracing::info!(%restarts_remaining, "Autoscale engine restarting...");
                restarts_remaining -= 1;
            }

            tracing::info!("autoscaling engine stopped.");
            Ok(())
        })
    });

    if let Err(ref err) = outcome {
        tracing::error!(error=?err, "autoscaling engine failed: {}", err);
    }

    tracing::info!("stopping autoscaler application.");
    outcome
}

/// Inject telemetry from settings. Current settings => telemetry include:
/// # Governance
/// - governance.rules.min_parallelism => min_parallelism
/// - governance.rules.max_parallelism => max_parallelism
/// - governance.rules.min_scaling_step => min_scaling_step
/// - governance.rules.max_scaling_step => max_scaling_step
/// - governance.rules.min_cluster_size => min_cluster_size
/// - governance.rules.max_cluster_size => max_cluster_size
/// # Planning
/// - plan.min_scaling_step => planning.min_scaling_step
/// - plan.direction_restart_secs => planning.rescale_restart_secs // table: key:secs
/// - plan.max_catch_up_secs => planning.max_catch_up_secs
/// - plan.recovery_valid_secs => planning.recovery_valid_secs
/// # environment conditions
/// some of these are defaults, updated during operation, others are placeholders for future.
/// - context_stub.all_sinks_healthy => all_sinks_healthy: placeholder
/// - => cluster.is_deploying: false default
/// - => cluster.last_deployment: now timestamp default
fn make_settings_sensor(settings: &Settings) -> BoxedTelemetrySource {
    let mut settings_telemetry: proctor::elements::telemetry::TableType = maplit::hashmap! {
        "min_parallelism".to_string() => settings.governance.rules.min_parallelism.into(),
        "max_parallelism".to_string() => settings.governance.rules.max_parallelism.into(),
        "min_scaling_step".to_string() => settings.governance.rules.min_scaling_step.into(),
        "max_scaling_step".to_string() => settings.governance.rules.max_scaling_step.into(),
        "min_cluster_size".to_string() => settings.governance.rules.min_cluster_size.into(),
        "max_cluster_size".to_string() => settings.governance.rules.max_cluster_size.into(),
    };
    settings_telemetry.extend(settings.governance.rules.custom.clone());

    // todo: consider planning context telemetry
    let max_catch_up = math::saturating_u64_to_u32(settings.plan.max_catch_up.as_secs());
    let recovery_valid = math::saturating_u64_to_u32(settings.plan.recovery_valid.as_secs());

    let rescale_restart: TableType = settings
        .plan
        .direction_restart
        .iter()
        .map(|(direction, duration)| {
            let secs = math::saturating_u64_to_u32(duration.as_secs());
            (direction.to_string(), secs.into())
        })
        .collect();

    settings_telemetry.extend(maplit::hashmap! {
        "planning.min_scaling_step".to_string() => settings.plan.min_scaling_step.into(),
        PLANNING__RESCALE_RESTART.to_string() => rescale_restart.into(),
        PLANNING__MAX_CATCH_UP.to_string() => max_catch_up.into(),
        PLANNING__RECOVERY_VALID.to_string() => recovery_valid.into(),
    });

    // todo: remove with proper eligibility context telemetry - see EligibilityContext
    settings_telemetry.extend(maplit::hashmap! {
        "all_sinks_healthy".to_string() => settings.context_stub.all_sinks_healthy.into(),
        "cluster.is_deploying".to_string() => false.into(),
        "cluster.last_deployment".to_string() => chrono::Utc::now().format(proctor::serde::date::FORMAT).to_string().into(),
        // "cluster.last_deployment".to_string() => format!("{}", settings.context_stub.cluster_last_deployment.format(proctor::serde::date::FORMAT)).into(),
    });

    Box::new(proctor::graph::stage::Sequence::new(
        "settings_telemetry",
        vec![settings_telemetry.into()],
    ))
}

/// makes a sensor the springline monitor uses to feed telemetry back into the sense phase.
/// # Returns
/// a tuple of the source stage and the tx_api used to push telemetry
fn make_monitor_sensor_and_api(_settings: &Settings) -> FeedbackSource {
    let src = proctor::graph::stage::ActorSource::new("monitor_sensor");
    let tx_api = src.tx_api();
    (Box::new(src), tx_api)
}

/// Start the pipelne closure wihtin the async runtime, which is set to a thread pool matching the
/// number of CPUs.
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

    // let console = console_subscriber::Builder::spawn(); //console_subscriber::ConsoleLayer::builder().spawn();
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(log_directives.as_ref()));
    let bunyan_formatting = tracing_bunyan_formatter::BunyanFormattingLayer::new(
        "springline".to_string(),
        std::io::stdout,
    );

    tracing_subscriber::registry::Registry::default()
        // .with(console)
        // .with(tracing_subscriber::fmt::layer())
        .with(env_filter)
        .with(tracing_bunyan_formatter::JsonStorageLayer)
        .with(bunyan_formatting)
}
