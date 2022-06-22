use crate::flink::MetricPortfolio;
use crate::phases;
use crate::phases::eligibility::EligibilityOutcome;
use crate::settings::DecisionSettings;
use crate::Result;
use proctor::elements::PolicySubscription;
use proctor::phases::policy_phase::PolicyPhase;
use proctor::phases::sense::{ClearinghouseSubscriptionAgent, SubscriptionChannel};
use std::fmt::Display;

mod context;
mod policy;
mod result;

pub use context::DecisionContext;
pub(crate) use policy::DECISION_SCALING_DECISION_COUNT_METRIC;
pub use policy::{DecisionPolicy, DecisionTemplateData};
pub use result::DecisionResult;
pub use result::DECISION_DIRECTION;
pub use result::{get_direction_and_reason, make_decision_transform};

pub type DecisionOutcome = DecisionResult<MetricPortfolio>;
pub type DecisionApi = proctor::elements::PolicyFilterApi<DecisionContext, DecisionTemplateData>;
pub type DecisionMonitor = proctor::elements::PolicyFilterMonitor<MetricPortfolio, DecisionContext>;
pub type DecisionEvent = proctor::elements::PolicyFilterEvent<MetricPortfolio, DecisionContext>;

pub type DecisionPhase = (
    Box<PolicyPhase<EligibilityOutcome, DecisionOutcome, DecisionContext, DecisionTemplateData>>,
    SubscriptionChannel<DecisionContext>,
);

#[tracing::instrument(level = "trace", skip(agent))]
pub async fn make_decision_phase<A>(settings: &DecisionSettings, agent: &mut A) -> Result<DecisionPhase>
where
    A: ClearinghouseSubscriptionAgent,
{
    let name = "decision";
    let policy = DecisionPolicy::new(settings);
    let subscription = policy.subscription(name, settings);
    let decision = Box::new(PolicyPhase::with_transform(name, policy, make_decision_transform(name)).await?);
    let channel = phases::subscribe_policy_phase(subscription, &decision, agent).await?;
    Ok((decision, channel))
}

#[derive(Debug, Display, Copy, Clone, PartialEq, Eq, Hash)]
pub enum ScaleDirection {
    Up,
    Down,
    None,
}
