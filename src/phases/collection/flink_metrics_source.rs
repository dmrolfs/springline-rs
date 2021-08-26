use proctor::phases::collection::{SourceSetting, TelemetrySource};

#[tracing::instrument(level = "info", skip(settings), fields(source_name=%source_name.as_ref()))]
pub async fn make_flink_metrics_source(
    source_name: impl AsRef<str>,
    settings: &SourceSetting,
) -> anyhow::Result<TelemetrySource> {
    // if let SourceSetting::RestApi(query) = setting {}
    todo!()
}
