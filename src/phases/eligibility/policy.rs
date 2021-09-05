use oso::{Oso, PolarClass};

use super::context::{ClusterStatus, EligibilityContext, TaskStatus};
use crate::phases::MetricCatalog;
use proctor::elements::{
    PolicySettings, PolicySource, PolicySubscription, QueryPolicy, QueryResult, Telemetry,
};
use proctor::error::PolicyError;
use proctor::ProctorContext;

// todo draft policy preample and/or default policy

#[derive(Debug)]
pub struct EligibilityPolicy(PolicySettings);

impl EligibilityPolicy {
    pub fn new(settings: &PolicySettings) -> Self {
        Self(settings.clone())
    }
}

impl PolicySubscription for EligibilityPolicy {
    type Requirements = EligibilityContext;
}

impl QueryPolicy for EligibilityPolicy {
    type Args = (Self::Item, Self::Context);
    type Context = EligibilityContext;
    type Item = MetricCatalog;

    fn initialize_policy_engine(&mut self, oso: &mut Oso) -> Result<(), PolicyError> {
        Telemetry::initialize_policy_engine(oso)?;

        oso.register_class(
            EligibilityContext::get_polar_class_builder()
                .add_method("custom", ProctorContext::custom)
                .build(),
        )?;
        oso.register_class(
            TaskStatus::get_polar_class_builder()
                .name("TaskStatus")
                .add_method(
                    "last_failure_within_seconds",
                    TaskStatus::last_failure_within_seconds,
                )
                .build(),
        )?;
        oso.register_class(
            ClusterStatus::get_polar_class_builder()
                .name("ClusterStatus")
                .add_method(
                    "last_deployment_within_seconds",
                    ClusterStatus::last_deployment_within_seconds,
                )
                .build(),
        )?;

        Ok(())
    }

    fn make_query_args(&self, item: &Self::Item, context: &Self::Context) -> Self::Args {
        (item.clone(), context.clone())
    }

    fn query_policy(&self, engine: &Oso, args: Self::Args) -> Result<QueryResult, PolicyError> {
        QueryResult::from_query(engine.query_rule("eligible", args)?)
    }

    fn policy_sources(&self) -> Vec<PolicySource> {
        self.0.policies.clone()
    }
}
