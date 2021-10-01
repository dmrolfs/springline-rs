use clap::Clap;
use proctor::tracing::{get_subscriber, init_subscriber};
use springline::engine::AutoscaleEngine;
use springline::settings::{CliOptions, Settings};
use springline::Result;
use std::future::Future;
use settings_loader::SettingsLoader;

fn main() -> Result<()> {
    let subscriber = get_subscriber("springline", "info");
    init_subscriber(subscriber);

    let main_span = tracing::info_span!("main");
    let _main_span_guard = main_span.enter();

    let options = CliOptions::parse();
    let settings = Settings::load(options)?;

    //todo assemble and start pipeline in entry
    start_pipeline(async move {
        let engine = AutoscaleEngine::new(settings).await?;
        engine.graph_handle.await??;
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
