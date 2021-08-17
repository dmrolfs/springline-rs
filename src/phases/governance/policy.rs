use std::collections::HashSet;

use oso::{Oso, PolarClass, PolarValue};
use serde::{Deserialize, Serialize};

use super::context::FlinkGovernanceContext;
use crate::phases::plan::FlinkScalePlan;
use proctor::elements::{
    PolicySettings, PolicySource, PolicySubscription, QueryPolicy, QueryResult, Telemetry,
};
use proctor::error::PolicyError;
use proctor::phases::collection::TelemetrySubscription;
use proctor::ProctorContext;

pub const ADJUSTED_TARGET: &'static str = "adjusted_target";

pub const GOVERNANCE_POLICY_PREAMBLE: &'static str = r#"
    accept(plan, context, adjusted_target)
        if accept_scale_up(plan, context, adjusted_target)
        or accept_scale_down(plan, context, adjusted_target);


    accept_scale_up(plan, context, adjusted_target)
        if check_scale_up(plan, context, adjusted)
        and context.max_cluster_size < adjusted
        and adjusted_target = context.max_cluster_size
        and cut;

    accept_scale_up(plan, context, adjusted_target)
        if check_scale_up(plan, context, adjusted_target)
        and cut;

    check_scale_up(plan, context, adjusted_target)
        if not veto(plan, context)
        and accept_step_up(plan, context, adjusted_target);


    accept_scale_down(plan, context, adjusted_target)
        if check_scale_down(plan, context, adjusted)
        and adjusted < context.min_cluster_size
        and adjusted_target = context.min_cluster_size
        and cut;

    accept_scale_down(plan, context, adjusted_target)
        if check_scale_down(plan, context, adjusted_target)
        and cut;

    check_scale_down(plan, context, adjusted_target)
        if not veto(plan, context)
        and accept_step_down(plan, context, adjusted_target);


    accept_step_up(plan, context, adjusted_target)
        if scale_up(plan)
        and (plan.target_nr_task_managers - plan.current_nr_task_managers) <= context.max_scaling_step
        and adjusted_target = plan.target_nr_task_managers;

    accept_step_up(plan, context, adjusted_target)
        if scale_up(plan)
        and context.max_scaling_step < (plan.target_nr_task_managers - plan.current_nr_task_managers)
        and adjusted_target = plan.current_nr_task_managers + context.max_scaling_step;


    accept_step_down(plan, context, adjusted_target)
        if scale_down(plan)
        and (plan.current_nr_task_managers - plan.target_nr_task_managers) <= context.max_scaling_step
        and adjusted_target = plan.target_nr_task_managers;

    accept_step_down(plan, context, adjusted_target)
        if scale_down(plan)
        and context.max_scaling_step < (plan.current_nr_task_managers - plan.target_nr_task_managers)
        and adjusted_target = plan.current_nr_task_managers - context.max_scaling_step;


    scale_up(plan) if plan.current_nr_task_managers < plan.target_nr_task_managers;
    scale_down(plan) if plan.target_nr_task_managers < plan.current_nr_task_managers;


    veto(plan, _context) if not scale_up(plan) and not scale_down(plan);
    "#;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FlinkGovernancePolicy {
    required_subscription_fields: HashSet<String>,
    optional_subscription_fields: HashSet<String>,
    policy_source: PolicySource,
}

impl FlinkGovernancePolicy {
    pub fn new(settings: &impl PolicySettings) -> Self {
        Self {
            required_subscription_fields: settings.required_subscription_fields(),
            optional_subscription_fields: settings.optional_subscription_fields(),
            policy_source: settings.source(),
        }
    }
}

impl PolicySubscription for FlinkGovernancePolicy {
    type Context = FlinkGovernanceContext;

    fn do_extend_subscription(&self, subscription: TelemetrySubscription) -> TelemetrySubscription {
        subscription
            .with_required_fields(self.required_subscription_fields.clone())
            .with_optional_fields(self.optional_subscription_fields.clone())
    }
}

impl QueryPolicy for FlinkGovernancePolicy {
    type Args = (Self::Item, Self::Context, PolarValue);
    type Context = FlinkGovernanceContext;
    type Item = FlinkScalePlan;

    fn load_policy_engine(&self, engine: &mut Oso) -> Result<(), PolicyError> {
        engine.load_str(GOVERNANCE_POLICY_PREAMBLE)?;
        self.policy_source.load_into(engine)
    }

    fn initialize_policy_engine(&mut self, engine: &mut Oso) -> Result<(), PolicyError> {
        Telemetry::initialize_policy_engine(engine)?;

        engine.register_class(
            FlinkGovernanceContext::get_polar_class_builder()
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
}
