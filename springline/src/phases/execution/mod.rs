use once_cell::sync::Lazy;
use proctor::graph::stage::{self, SinkStage};
use prometheus::{IntCounterVec, Opts};

use crate::phases::governance::GovernanceOutcome;
use crate::settings::ExecutionSettings;
use crate::Result;

pub(crate) static EXECUTION_SCALE_ACTION_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new("execution_scale_action_count", "Count of action taken to target sizes"),
        &["current_nr_task_managers", "target_nr_task_managers"],
    )
    .expect("failed creating execution_scale_action_count metric")
});

#[tracing::instrument(level = "info", skip(_settings))]
pub async fn make_execution_phase(_settings: &ExecutionSettings) -> Result<Box<dyn SinkStage<GovernanceOutcome>>> {
    let execution: Box<dyn SinkStage<GovernanceOutcome>> =
        Box::new(stage::Foreach::new("execution", |plan: GovernanceOutcome| {
            EXECUTION_SCALE_ACTION_COUNT
                .with_label_values(&[
                    plan.current_nr_task_managers.to_string().as_str(),
                    plan.target_nr_task_managers.to_string().as_str(),
                ])
                .inc();
            tracing::warn!(scale_plan=?plan, "EXECUTE SCALE PLAN!");
        }));
    Ok(execution)
}
