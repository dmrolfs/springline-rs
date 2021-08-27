use crate::phases::governance::GoveranceOutcome;
use crate::settings::Settings;
use proctor::graph::stage::SinkStage;

#[tracing::instrument(level = "info", skip(_settings))]
pub async fn make_execution_phase(
    _settings: &Settings,
) -> anyhow::Result<Box<dyn SinkStage<GoveranceOutcome>>> {
    todo!()
}
