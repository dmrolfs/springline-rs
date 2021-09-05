use oso::{Oso, PolarClass, PolarValue};
use serde::{Deserialize, Serialize};

use super::context::DecisionContext;
use crate::phases::decision::result::DECISION_BINDING;
use crate::phases::MetricCatalog;
use proctor::elements::{
    PolicySettings, PolicySource, PolicySubscription, QueryPolicy, QueryResult, Telemetry,
};
use proctor::error::PolicyError;
use proctor::ProctorContext;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecisionPolicy(PolicySettings);

impl DecisionPolicy {
    pub fn new(settings: &PolicySettings) -> Self {
        Self(settings.clone())
    }
}

impl PolicySubscription for DecisionPolicy {
    type Requirements = DecisionContext;
}

impl QueryPolicy for DecisionPolicy {
    type Args = (Self::Item, Self::Context, PolarValue);
    type Context = DecisionContext;
    type Item = MetricCatalog;

    fn initialize_policy_engine(&mut self, engine: &mut Oso) -> Result<(), PolicyError> {
        Telemetry::initialize_policy_engine(engine)?;

        engine.register_class(
            DecisionContext::get_polar_class_builder()
                .add_method("custom", ProctorContext::custom)
                .build(),
        )?;

        Ok(())
    }

    fn make_query_args(&self, item: &Self::Item, context: &Self::Context) -> Self::Args {
        (
            item.clone(),
            context.clone(),
            PolarValue::Variable(DECISION_BINDING.to_string()),
        )
    }

    fn query_policy(&self, engine: &Oso, args: Self::Args) -> Result<QueryResult, PolicyError> {
        QueryResult::from_query(engine.query_rule("scale", args)?)
    }

    fn policy_sources(&self) -> Vec<PolicySource> {
        self.0.policies.clone()
    }
}
