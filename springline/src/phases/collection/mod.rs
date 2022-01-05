use std::collections::HashMap;

use pretty_snowflake::MachineNode;
use proctor::elements::Telemetry;
use proctor::graph::stage::tick::TickApi;
use proctor::graph::stage::SourceStage;
use proctor::phases::collection::builder::CollectBuilder;
use proctor::phases::collection::{Collect, SourceSetting, TelemetrySource};
use proctor::SharedString;

use crate::phases::MetricCatalog;
use crate::settings::CollectionSettings;
use crate::Result;

pub mod flink;

#[tracing::instrument(level = "info", skip(name, settings, auxiliary_sources))]
pub async fn make_collection_phase(
    name: impl Into<SharedString>, settings: &CollectionSettings,
    auxiliary_sources: Vec<Box<dyn SourceStage<Telemetry>>>, machine_node: MachineNode,
) -> Result<(CollectBuilder<MetricCatalog>, Option<TickApi>)> {
    let mut sources = do_make_telemetry_sources(&settings.sources, auxiliary_sources).await?;

    let mut flink_source = flink::make_flink_metrics_source("metrics", &settings.flink).await?;
    let tx_stop_flink_source = flink_source.tx_stop.take();
    if let Some(source) = flink_source.stage.take() {
        sources.push(source);
    }

    Ok((Collect::builder(name, sources, machine_node), tx_stop_flink_source))
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
