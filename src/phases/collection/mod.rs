use crate::phases::MetricCatalog;
use crate::settings::CollectionSettings;
use crate::Result;
use proctor::elements::Telemetry;
use proctor::graph::stage::SourceStage;
use proctor::phases::collection::builder::CollectBuilder;
use proctor::phases::collection::{Collect, SourceSetting, TelemetrySource};
use std::collections::HashMap;

pub mod flink_metrics_source;

#[tracing::instrument(level = "info", skip(settings, auxiliary_source))]
pub async fn make_collection_phase(
    settings: &CollectionSettings, auxiliary_source: Option<Box<dyn SourceStage<Telemetry>>>,
) -> Result<CollectBuilder<MetricCatalog>> {
    let name = "collection";
    let sources = do_make_telemetry_sources(&settings.sources, auxiliary_source).await?;
    Ok(Collect::builder(name, sources))
}

#[tracing::instrument(level = "info", skip())]
async fn do_make_telemetry_sources(
    settings: &HashMap<String, SourceSetting>, auxiliary: Option<Box<dyn SourceStage<Telemetry>>>,
) -> Result<Vec<Box<dyn SourceStage<Telemetry>>>> {
    let mut sources = TelemetrySource::collect_from_settings::<MetricCatalog>(settings)
        .await?
        .into_iter()
        .flat_map(|mut s| s.take())
        .map(|(s, _api)| s)
        .collect::<Vec<_>>();

    if let Some(aux) = auxiliary {
        sources.push(aux);
    }

    Ok(sources)
}
