use clap::Clap;
use proctor::tracing::{get_subscriber, init_subscriber};
use settings_loader::SettingsLoader;
use springline::engine::Autoscaler;
use springline::settings::{CliOptions, Settings};
use springline::Result;
use std::future::Future;

fn main() -> Result<()> {
    let subscriber = get_subscriber("springline", "info");
    init_subscriber(subscriber);

    let main_span = tracing::info_span!("main");
    let _main_span_guard = main_span.enter();

    let options = CliOptions::parse();
    let settings = Settings::load(options)?;

    //todo assemble and start pipeline in entry
    start_pipeline(async move {
        Autoscaler::builder("springline")
            .finish(settings)
            .await?
            .run()
            .block_for_completion()
            .await?;
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
