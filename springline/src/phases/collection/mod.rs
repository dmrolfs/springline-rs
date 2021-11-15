use std::collections::HashMap;

use pretty_snowflake::MachineNode;
use proctor::elements::Telemetry;
use proctor::graph::stage::SourceStage;
use proctor::phases::collection::builder::CollectBuilder;
use proctor::phases::collection::{Collect, SourceSetting, TelemetrySource};

use crate::phases::MetricCatalog;
use crate::settings::CollectionSettings;
use crate::Result;

pub mod flink_metrics_source;

#[tracing::instrument(level = "info", skip(settings, auxiliary_sources))]
pub async fn make_collection_phase(
    settings: &CollectionSettings, auxiliary_sources: Vec<Box<dyn SourceStage<Telemetry>>>, machine_node: MachineNode,
) -> Result<CollectBuilder<MetricCatalog>> {
    let name = "collection";
    let sources = do_make_telemetry_sources(&settings.sources, auxiliary_sources).await?;
    Ok(Collect::builder(name, sources, machine_node))
}

#[tracing::instrument(level = "info", skip())]
async fn do_make_telemetry_sources(
    settings: &HashMap<String, SourceSetting>, auxiliary: Vec<Box<dyn SourceStage<Telemetry>>>,
) -> Result<Vec<Box<dyn SourceStage<Telemetry>>>> {
    let mut sources = TelemetrySource::collect_from_settings::<MetricCatalog>(settings)
        .await?
        .into_iter()
        .flat_map(|mut s| s.take())
        .map(|(s, _api)| s)
        .collect::<Vec<_>>();

    auxiliary.into_iter().for_each(|s| sources.push(s));

    Ok(sources)
}
