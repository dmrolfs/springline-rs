use proctor::phases::collection::{SourceSetting, TelemetrySource};

#[tracing::instrument(level = "info", skip(_source_name, _settings), fields(source_name=%_source_name.as_ref()))]
pub async fn make_flink_metrics_source(
    _source_name: impl AsRef<str>,
    _settings: &SourceSetting,
) -> anyhow::Result<TelemetrySource> {
    // if let SourceSetting::RestApi(query) = setting {}
    todo!()
}
