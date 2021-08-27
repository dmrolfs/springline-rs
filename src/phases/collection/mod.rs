use crate::phases::MetricCatalog;
use crate::settings::Settings;
use proctor::graph::stage::SourceStage;
use proctor::phases::collection::{ClearinghouseApi, SourceSetting, TelemetrySource};

pub mod flink_metrics_source;

#[tracing::instrument(level = "info", skip(_settings))]
pub async fn make_collection_phase(
    _settings: &Settings,
) -> anyhow::Result<(Box<dyn SourceStage<MetricCatalog>>, ClearinghouseApi)> {
    // let sources = for (ref name, ref source_setting) in settings.collection_sources {
    //     let (telemetry_source, tx_trigger) = match source_setting {
    //         SourceSetting::RestApi(_) => {
    //             let (src, api) = make_telemetry_rest_api_source(name, source_setting).await?;
    //             (src, Some(api))
    //         }
    //
    //         SourceSetting::Csv { path: _ } => {
    //             (make_telemetry_cvs_source(name, source_setting)?, None)
    //         }
    //     };
    // };
    todo!()
}

#[tracing::instrument(level = "info", skip())]
fn make_telemetry_source(
    _source_name: &String,
    _setting: &SourceSetting,
) -> anyhow::Result<TelemetrySource> {
    // match source_name.as_str() {
    //
    // }
    todo!()
}
