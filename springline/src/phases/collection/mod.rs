use std::collections::HashMap;

use pretty_snowflake::MachineNode;
use proctor::elements::Telemetry;
use proctor::graph::stage::tick::{Tick, TickApi};
use proctor::graph::stage::{SourceStage, WithApi};
use proctor::phases::collection::builder::CollectBuilder;
use proctor::phases::collection::{Collect, SourceSetting, TelemetrySource};

use crate::phases::MetricCatalog;
use crate::settings::CollectionSettings;
use crate::Result;

pub mod flink;

#[tracing::instrument(level = "info", skip(name, settings, auxiliary_sources))]
pub async fn make_collection_phase(
    name: &str, settings: &CollectionSettings, auxiliary_sources: Vec<Box<dyn SourceStage<Telemetry>>>,
    machine_node: MachineNode,
) -> Result<(CollectBuilder<MetricCatalog>, TickApi)> {
    let mut sources = do_make_telemetry_sources(&settings.sources, auxiliary_sources).await?;

    let scheduler = Tick::new(
        "springline_flink",
        settings.flink.metrics_initial_delay,
        settings.flink.metrics_interval,
        (),
    );
    let tx_scheduler_api = scheduler.tx_api();

    let flink_source = flink::make_flink_metrics_source("springline", Box::new(scheduler), &settings.flink).await?;

    sources.push(flink_source);

    Ok((
        Collect::builder(name.to_string(), sources, machine_node),
        tx_scheduler_api,
    ))
}

#[tracing::instrument(level = "info", skip())]
async fn do_make_telemetry_sources(
    settings: &HashMap<String, SourceSetting>, auxiliary: Vec<Box<dyn SourceStage<Telemetry>>>,
) -> Result<Vec<Box<dyn SourceStage<Telemetry>>>> {
    let mut sources = TelemetrySource::collect_from_settings::<MetricCatalog>(settings)
        .await?
        .into_iter()
        .flat_map(|mut s| s.stage.take())
        .collect::<Vec<_>>();

    auxiliary.into_iter().for_each(|s| sources.push(s));

    Ok(sources)
}
