use std::collections::HashSet;

use oso::{Oso, PolarClass, PolarValue};
use serde::{Deserialize, Serialize};

use super::context::FlinkDecisionContext;
use crate::phases::MetricCatalog;
use proctor::elements::{
    PolicySettings, PolicySource, PolicySubscription, QueryPolicy, QueryResult, Telemetry,
};
use proctor::error::PolicyError;
use proctor::phases::collection::TelemetrySubscription;
use proctor::phases::decision::DECISION_POLICY_PREAMBLE;
use proctor::ProctorContext;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FlinkDecisionPolicy {
    settings: PolicySettings,
}

impl FlinkDecisionPolicy {
    pub fn new(settings: &PolicySettings) -> Self {
        Self {
            settings: settings.clone(),
        }
    }
}

impl PolicySubscription for FlinkDecisionPolicy {
    type Requirements = FlinkDecisionContext;

    // fn do_extend_subscription(&self, subscription: TelemetrySubscription) -> TelemetrySubscription {
    //     subscription
    //         .with_required_fields(self.required_subscription_fields.clone())
    //         .with_optional_fields(self.optional_subscription_fields.clone())
    // }
}

impl QueryPolicy for FlinkDecisionPolicy {
    type Args = (Self::Item, Self::Context, PolarValue);
    type Context = FlinkDecisionContext;
    type Item = MetricCatalog;

    fn load_policy_engine(&self, engine: &mut Oso) -> Result<(), PolicyError> {
        engine.load_str(DECISION_POLICY_PREAMBLE)?;
        self.settings.source.load_into(engine)?;
        Ok(())
    }

    fn initialize_policy_engine(&mut self, engine: &mut Oso) -> Result<(), PolicyError> {
        Telemetry::initialize_policy_engine(engine)?;

        engine.register_class(
            FlinkDecisionContext::get_polar_class_builder()
                .add_method("custom", ProctorContext::custom)
                .build(),
        )?;

        Ok(())
    }

    fn make_query_args(&self, item: &Self::Item, context: &Self::Context) -> Self::Args {
        (
            item.clone(),
            context.clone(),
            PolarValue::Variable("direction".to_string()),
        )
    }

    fn query_policy(&self, engine: &Oso, args: Self::Args) -> Result<QueryResult, PolicyError> {
        QueryResult::from_query(engine.query_rule("scale", args)?)
    }
}
