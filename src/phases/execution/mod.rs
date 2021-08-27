use crate::phases::governance::GoveranceOutcome;
use crate::settings::Settings;
use crate::Result;
use proctor::graph::stage::SinkStage;

#[tracing::instrument(level = "info", skip(_settings))]
pub async fn make_execution_phase(
    _settings: &Settings,
) -> Result<Box<dyn SinkStage<GoveranceOutcome>>> {
    todo!()
}
