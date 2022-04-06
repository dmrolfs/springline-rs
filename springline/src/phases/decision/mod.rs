mod context;
mod policy;
mod result;

pub use context::DecisionContext;
pub(crate) use policy::DECISION_SCALING_DECISION_COUNT_METRIC;
pub use policy::{DecisionPolicy, DecisionTemplateData};
pub use result::make_decision_transform;
pub use result::DecisionResult;
pub use result::DECISION_DIRECTION;

use crate::model::MetricPortfolio;
use proctor::elements::PolicySubscription;
use proctor::phases::policy_phase::PolicyPhase;
use proctor::phases::sense::{ClearinghouseSubscriptionAgent, SubscriptionChannel};
use proctor::SharedString;

use crate::phases;
use crate::phases::eligibility::EligibilityOutcome;
use crate::settings::DecisionSettings;
use crate::Result;

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
    let name: SharedString = "decision".into();
    let policy = DecisionPolicy::new(settings);
    let subscription = policy.subscription(name.as_ref(), settings);
    let decision =
        Box::new(PolicyPhase::with_transform(name.clone(), policy, make_decision_transform(name.into_owned())).await?);

    let channel = phases::subscribe_policy_phase(subscription, &decision, agent).await?;
    Ok((decision, channel))
}
