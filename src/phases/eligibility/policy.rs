use std::collections::HashSet;

use oso::{Oso, PolarClass};

use super::context::{ClusterStatus, FlinkEligibilityContext, TaskStatus};
use crate::phases::MetricCatalog;
use proctor::elements::{
    PolicySettings, PolicySource, PolicySubscription, QueryPolicy, QueryResult, Telemetry,
};
use proctor::error::PolicyError;
use proctor::phases::collection::TelemetrySubscription;
use proctor::ProctorContext;

// todo draft policy preample and/or default policy

#[derive(Debug)]
pub struct EligibilityPolicy {
    required_subscription_fields: HashSet<String>,
    optional_subscription_fields: HashSet<String>,
    policy_source: PolicySource,
}

impl EligibilityPolicy {
    pub fn new(settings: &impl PolicySettings) -> Self {
        Self {
            required_subscription_fields: settings.required_subscription_fields(),
            optional_subscription_fields: settings.optional_subscription_fields(),
            policy_source: settings.source(),
        }
    }
}

impl PolicySubscription for EligibilityPolicy {
    type Context = FlinkEligibilityContext;

    fn do_extend_subscription(&self, subscription: TelemetrySubscription) -> TelemetrySubscription {
        subscription
            .with_required_fields(self.required_subscription_fields.clone())
            .with_optional_fields(self.optional_subscription_fields.clone())
    }
}

impl QueryPolicy for EligibilityPolicy {
    type Args = (Self::Item, Self::Context);
    type Context = FlinkEligibilityContext;
    type Item = MetricCatalog;

    fn load_policy_engine(&self, oso: &mut Oso) -> Result<(), PolicyError> {
        self.policy_source.load_into(oso)
    }

    fn initialize_policy_engine(&mut self, oso: &mut Oso) -> Result<(), PolicyError> {
        Telemetry::initialize_policy_engine(oso)?;

        oso.register_class(
            FlinkEligibilityContext::get_polar_class_builder()
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
}
