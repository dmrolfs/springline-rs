use proctor::elements::PolicyOutcome;
use proctor::graph::stage;
use proctor::graph::stage::ThroughStage;

use super::policy::ADJUSTED_TARGET;
use crate::phases::governance::context::GovernanceContext;
use crate::phases::plan::ScalePlan;

type Item = ScalePlan;
type Context = GovernanceContext;

pub fn make_governance_transform(name: impl Into<String>) -> impl ThroughStage<PolicyOutcome<Item, Context>, Item> {
    let stage = stage::FilterMap::new(name, move |outcome: PolicyOutcome<Item, Context>| {
        let adjusted_target = outcome
            .policy_results
            .binding(ADJUSTED_TARGET)
            .map(|adjusted_targets: Vec<u16>| {
                if !adjusted_targets.is_empty() {
                    Some(itertools::min(adjusted_targets).unwrap())
                } else {
                    None
                }
            });

        let transform_span = tracing::info_span!(
            "apply governance adjustments",
            item=?outcome.item, policy_results=?outcome.policy_results, ?adjusted_target
        );
        let _transform_span_guard = transform_span.enter();

        match (
            adjusted_target,
            outcome.item.current_nr_task_managers,
            outcome.item.target_nr_task_managers,
        ) {
            (Err(err), _current, _target) => {
                tracing::error!(error=?err, "error during policy review - dropping plan.");
                None
            },

            (Ok(None), _current, _target) => {
                tracing::error!("{} binding is empty - dropping plan.", ADJUSTED_TARGET);
                None
            },

            (Ok(Some(adjusted)), current, _target) if adjusted == current => {
                tracing::warn!("final plan does not affect cluster change - dropping.");
                None
            },

            (Ok(Some(adjusted)), _current, target) if adjusted == target => Some(outcome.item),

            (Ok(Some(adjusted)), _current, _target) => {
                tracing::warn!("governance accepted plan with adjustment.");
                Some(ScalePlan { target_nr_task_managers: adjusted, ..outcome.item })
            },
        }
    });

    stage.with_block_logging()
}
