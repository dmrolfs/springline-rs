use oso::{Oso, PolarClass, PolarValue};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use super::context::GovernanceContext;
use crate::phases::plan::ScalePlan;
use crate::phases::UpdateMetrics;
use crate::settings::GovernanceSettings;
use proctor::elements::{PolicySource, PolicySubscription, QueryPolicy, QueryResult, Telemetry};
use proctor::error::PolicyError;
use proctor::phases::collection::TelemetrySubscription;
use proctor::{ProctorContext, SharedString};

pub const ADJUSTED_TARGET: &'static str = "adjusted_target";

// pub const GOVERNANCE_POLICY_PREAMBLE: &'static str = r#"
//     accept(plan, context, adjusted_target)
//         if accept_scale_up(plan, context, adjusted_target)
//         or accept_scale_down(plan, context, adjusted_target);
//
//
//     accept_scale_up(plan, context, adjusted_target)
//         if check_scale_up(plan, context, adjusted)
//         and context.max_cluster_size < adjusted
//         and adjusted_target = context.max_cluster_size
//         and cut;
//
//     accept_scale_up(plan, context, adjusted_target)
//         if check_scale_up(plan, context, adjusted_target)
//         and cut;
//
//     check_scale_up(plan, context, adjusted_target)
//         if not veto(plan, context)
//         and accept_step_up(plan, context, adjusted_target);
//
//
//     accept_scale_down(plan, context, adjusted_target)
//         if check_scale_down(plan, context, adjusted)
//         and adjusted < context.min_cluster_size
//         and adjusted_target = context.min_cluster_size
//         and cut;
//
//     accept_scale_down(plan, context, adjusted_target)
//         if check_scale_down(plan, context, adjusted_target)
//         and cut;
//
//     check_scale_down(plan, context, adjusted_target)
//         if not veto(plan, context)
//         and accept_step_down(plan, context, adjusted_target);
//
//
//     accept_step_up(plan, context, adjusted_target)
//         if scale_up(plan)
//         and (plan.target_nr_task_managers - plan.current_nr_task_managers) <= context.max_scaling_step
//         and adjusted_target = plan.target_nr_task_managers;
//
//     accept_step_up(plan, context, adjusted_target)
//         if scale_up(plan)
//         and context.max_scaling_step < (plan.target_nr_task_managers - plan.current_nr_task_managers)
//         and adjusted_target = plan.current_nr_task_managers + context.max_scaling_step;
//
//
//     accept_step_down(plan, context, adjusted_target)
//         if scale_down(plan)
//         and (plan.current_nr_task_managers - plan.target_nr_task_managers) <= context.max_scaling_step
//         and adjusted_target = plan.target_nr_task_managers;
//
//     accept_step_down(plan, context, adjusted_target)
//         if scale_down(plan)
//         and context.max_scaling_step < (plan.current_nr_task_managers - plan.target_nr_task_managers)
//         and adjusted_target = plan.current_nr_task_managers - context.max_scaling_step;
//
//
//     scale_up(plan) if plan.current_nr_task_managers < plan.target_nr_task_managers;
//     scale_down(plan) if plan.target_nr_task_managers < plan.current_nr_task_managers;
//
//
//     veto(plan, _context) if not scale_up(plan) and not scale_down(plan);
// "#;

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct GovernanceTemplateData;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct GovernancePolicy {
    pub required_subscription_fields: HashSet<SharedString>,
    pub optional_subscription_fields: HashSet<SharedString>,
    pub sources: Vec<PolicySource>,
    pub template_data: Option<GovernanceTemplateData>,
}

impl GovernancePolicy {
    pub fn new(settings: &GovernanceSettings) -> Self {
        let required_subscription_fields = settings
            .required_subscription_fields
            .iter()
            .map(|f| SharedString::from(f.to_string()))
            .collect();
        let optional_subscription_fields = settings
            .optional_subscription_fields
            .iter()
            .map(|f| SharedString::from(f.to_string()))
            .collect();
        Self {
            required_subscription_fields,
            optional_subscription_fields,
            sources: settings.policies.clone(),
            template_data: settings.template_data.clone(),
        }
    }
}

impl PolicySubscription for GovernancePolicy {
    type Requirements = GovernanceContext;

    fn do_extend_subscription(&self, subscription: TelemetrySubscription) -> TelemetrySubscription {
        let update_fn = Self::Requirements::update_metrics_for(subscription.name());
        subscription.with_update_metrics_fn(update_fn)
    }
}

impl QueryPolicy for GovernancePolicy {
    type Args = (Self::Item, Self::Context, PolarValue);
    type Context = GovernanceContext;
    type Item = ScalePlan;

    fn initialize_policy_engine(&mut self, engine: &mut Oso) -> Result<(), PolicyError> {
        Telemetry::initialize_policy_engine(engine)?;

        engine.register_class(
            GovernanceContext::get_polar_class_builder()
                .add_method("custom", ProctorContext::custom)
                .build(),
        )?;

        Ok(())
    }

    fn make_query_args(&self, item: &Self::Item, context: &Self::Context) -> Self::Args {
        (
            item.clone(),
            context.clone(),
            PolarValue::Variable(ADJUSTED_TARGET.to_string()),
        )
    }

    fn query_policy(&self, engine: &Oso, args: Self::Args) -> Result<QueryResult, PolicyError> {
        QueryResult::from_query(engine.query_rule("accept", args)?)
    }

    type TemplateData = GovernanceTemplateData;

    fn base_template_name() -> &'static str {
        "governance"
    }

    fn policy_template_data(&self) -> Option<&Self::TemplateData> {
        self.template_data.as_ref()
    }

    fn policy_template_data_mut(&mut self) -> Option<&mut Self::TemplateData> {
        self.template_data.as_mut()
    }

    fn sources(&self) -> &[PolicySource] {
        self.sources.as_slice()
    }

    fn sources_mut(&mut self) -> &mut Vec<PolicySource> {
        &mut self.sources
    }
}
