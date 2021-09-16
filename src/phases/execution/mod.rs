use crate::phases::governance::GovernanceOutcome;
use crate::settings::ExecutionSettings;
use crate::Result;
use proctor::graph::stage::SinkStage;

#[tracing::instrument(level = "info", skip(_settings))]
pub async fn make_execution_phase(
    _settings: &ExecutionSettings,
) -> Result<Box<dyn SinkStage<GovernanceOutcome>>> {
    let logged = proctor::graph::stage::LoggedSink::new("execution");
    let execution: Box<dyn SinkStage<GovernanceOutcome>> = Box::new(logged);
    Ok(execution)
}
