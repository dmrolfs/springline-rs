mod context;
mod policy;
mod result;

pub use context::GovernanceContext;
pub(crate) use context::{
    GOVERNANCE_CTX_MAX_CLUSTER_SIZE, GOVERNANCE_CTX_MAX_SCALING_STEP, GOVERNANCE_CTX_MIN_CLUSTER_SIZE,
};
pub use policy::{GovernancePolicy, GovernanceTemplateData};
pub use result::make_governance_transform;

use proctor::elements::{PolicyFilterEvent, PolicySubscription};
use proctor::phases::policy_phase::PolicyPhase;
use proctor::phases::sense::{ClearinghouseSubscriptionAgent, SubscriptionChannel};
use proctor::SharedString;

use crate::phases::{
    self,
    plan::{PlanningOutcome, ScalePlan},
};
use crate::settings::GovernanceSettings;
use crate::Result;

pub type GovernanceOutcome = PlanningOutcome;

pub type GovernanceApi = proctor::elements::PolicyFilterApi<GovernanceContext, GovernanceTemplateData>;
pub type GovernanceMonitor = proctor::elements::PolicyFilterMonitor<ScalePlan, GovernanceContext>;
pub type GovernancePhase = (
    Box<PolicyPhase<PlanningOutcome, GovernanceOutcome, GovernanceContext, GovernanceTemplateData>>,
    SubscriptionChannel<GovernanceContext>,
);
pub type GovernanceEvent = PolicyFilterEvent<PlanningOutcome, GovernanceContext>;

#[tracing::instrument(level = "trace", skip(agent))]
pub async fn make_governance_phase<A>(settings: &GovernanceSettings, agent: &mut A) -> Result<GovernancePhase>
where
    A: ClearinghouseSubscriptionAgent,
{
    let name: SharedString = "governance".into();
    let policy = GovernancePolicy::new(settings);
    let subscription = policy.subscription(name.as_ref(), &settings.policy);
    //todo: inject a "monitor relay" that captures gov's policy_filter events and passes along blocked,
    // BUT holds Passed until data passes transform, when it sends the Passed event.
    // May need to intro "builder" to policy phase to access filter's monitor to decorate.
    let governance = Box::new(
        PolicyPhase::with_transform(name.clone(), policy, make_governance_transform(name.into_owned())).await?,
    );

    let channel = phases::subscribe_policy_phase(subscription, &governance, agent).await?;
    Ok((governance, channel))
}
