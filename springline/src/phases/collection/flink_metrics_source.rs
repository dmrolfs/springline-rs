use once_cell::sync::Lazy;
use proctor::graph::stage::WithApi;
use proctor::phases::collection::TelemetrySource;
use reqwest_middleware::ClientBuilder;
use reqwest_retry::policies::ExponentialBackoff;
use reqwest_retry::RetryTransientMiddleware;

use crate::Result;
use crate::settings::{FlinkMetricOrder, FlinkSettings};

static STD_METRIC_ORDERS: Lazy<Vec<FlinkMetricOrder>> = Lazy::new(|| {
    use crate::settings::{FlinkScope::*, FlinkMetricAggregatedValue::*};

    [
        (Jobs, "uptime", Max),                          // does not work w reactive mode
        (Jobs, "numRestarts", Max),
        (Jobs, "numberOfCompletedCheckpoints", Max),    // does not work w reactive mode
        (Jobs, "numberOfFailedCheckpoints", Max),       // does not work w reactive mode
        (Task, "numRecordsInPerSecond", Max),
        (Task, "numRecordsOutPerSecond", Max),
        (TaskManagers, "Status.JVM.CPU.LOAD", Max),
        (TaskManagers, "Status.JVM.Memory.Heap.Used", Max),
        (TaskManagers, "Status.JVM.Memory.Heap.Committed", Max),
        (TaskManagers, "Status.JVM.Threads.Count", Max),
        (Task, "buffers.inputQueueLength", Max), // verify,
        (Task, "buffers.inPoolUsage", Max), // verify,
        (Task, "buffers.outputQueueLength", Max), // verify,
        (Task, "buffers.outPoolUsage", Max), // verify,
    ]
        .into_iter()
        .map(|(s, m, a)| FlinkMetricOrder(s, m.to_string(), a))
        .collect()
});

#[tracing::instrument(level = "info", skip(name, settings), fields(source_name=%name.as_ref()))]
pub async fn make_flink_metrics_source(
    name: impl AsRef<str>, settings: &FlinkSettings,
) -> Result<TelemetrySource> {
    // scheduler
    let tick = proctor::graph::stage::Tick::new(
        format!("flink_source_{}_tick", name.as_ref()),
        settings.metrics_initial_delay,
        settings.metrics_interval,
        (),
    );
    let tx_tick_api = tick.tx_api();

    // flink metric rest generator
    let headers = settings.header_map()?;
    let client = reqwest::Client::builder()
        .default_headers(headers)
        .build()?;
    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(settings.max_retries);
    let client = ClientBuilder::new(client)
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build();

    // let base_url = settings.b
    todo!()
}
