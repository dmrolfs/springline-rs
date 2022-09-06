use proctor::elements::PolicyOutcome;
use proctor::graph::stage;
use proctor::graph::stage::ThroughStage;

use super::policy::{ADJUSTED_TARGET_NR_TASK_MANAGERS, ADJUSTED_TARGET_PARALLELISM};
use crate::phases::governance::context::GovernanceContext;
use crate::phases::plan::ScalePlan;

type Item = ScalePlan;
type Context = GovernanceContext;

#[allow(clippy::cognitive_complexity)]
pub fn make_governance_transform(
    name: impl Into<String>,
) -> impl ThroughStage<PolicyOutcome<Item, Context>, Item> {
    let stage = stage::FilterMap::new(name, move |outcome: PolicyOutcome<Item, Context>| {
        let adjusted_target_parallelism = outcome
            .policy_results
            .binding(ADJUSTED_TARGET_PARALLELISM)
            .map(
                |targets: Vec<u32>| {
                    if !targets.is_empty() {
                        itertools::min(targets)
                    } else {
                        None
                    }
                },
            );

        let adjusted_target_nr_task_managers =
            outcome.policy_results.binding(ADJUSTED_TARGET_NR_TASK_MANAGERS).map(
                |targets: Vec<u32>| {
                    if !targets.is_empty() {
                        itertools::min(targets)
                    } else {
                        None
                    }
                },
            );

        let transform_span = tracing::info_span!(
            "apply governance adjustments",
            scale_plan=?outcome.item, policy_results=?outcome.policy_results, ?adjusted_target_parallelism, ?adjusted_target_nr_task_managers
        );
        let _transform_span_guard = transform_span.enter();

        let plan = &outcome.item;

        match (
            adjusted_target_parallelism,
            adjusted_target_nr_task_managers,
        ) {
            (Err(err), _) => {
                tracing::error!(error=?err, "error w parallelism during policy review - dropping plan.");
                None
            },

            (_, Err(err)) => {
                tracing::error!(error=?err, "error w nr task managers during policy review - dropping plan.");
                None
            },

            (Ok(None), _) => {
                tracing::error!(
                    "{} binding is empty - dropping plan.",
                    ADJUSTED_TARGET_PARALLELISM
                );
                None
            },

            (_, Ok(None)) => {
                tracing::error!(
                    "{} binding is empty - dropping plan.",
                    ADJUSTED_TARGET_NR_TASK_MANAGERS
                );
                None
            },

            (Ok(Some(adjusted_job_parallelism)), _)
                if adjusted_job_parallelism == plan.current_job_parallelism =>
            {
                tracing::warn!(%adjusted_job_parallelism, current_job_parallelism=%plan.current_job_parallelism,
                    "final plan does not affect cluster change - dropping."
                );
                None
            }

            (Ok(Some(adj_p)), Ok(Some(adj_tms)))
                if adj_p != plan.target_job_parallelism
                    || adj_tms != plan.target_nr_task_managers =>
            {
                tracing::info!(
                    ?plan, adjusted_job_parallelism=%adj_p, adjusted_nr_task_managers=%adj_tms,
                    "governance accepted plan with adjustment."
                );
                Some(ScalePlan {
                    target_job_parallelism: adj_p,
                    target_nr_task_managers: adj_tms,
                    ..outcome.item
                })
            },

            (Ok(Some(_)), Ok(Some(_))) => {
                tracing::info!(scale_plan=?outcome.item, "governance accepted plan without adjustment");
                Some(outcome.item)
            },
        }
    });

    stage.with_block_logging()
}
