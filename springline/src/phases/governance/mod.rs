mod context;
mod policy;
mod result;
mod stage;

pub use context::GovernanceContext;
use proctor::elements::PolicyFilterEvent;
use proctor::graph::{Connect, SourceShape};
use proctor::phases::sense::{ClearinghouseSubscriptionAgent, SubscriptionChannel};
pub use stage::GovernanceStage;

use crate::phases::{self, plan::PlanningOutcome};
use crate::settings::GovernanceSettings;
use crate::Result;

pub type GovernanceOutcome = PlanningOutcome;

// pub type GovernanceApi =
//     proctor::elements::PolicyFilterApi<GovernanceContext, GovernanceTemplateData>;
pub type GovernanceMonitor<T> = proctor::elements::PolicyFilterMonitor<T, GovernanceContext>;
pub type GovernancePhase = (
    Box<GovernanceStage<PlanningOutcome>>,
    SubscriptionChannel<GovernanceContext>,
);
// pub type GovernancePhase = (
//     Box<PolicyPhase<PlanningOutcome, GovernanceOutcome, GovernanceContext, GovernanceTemplateData>>,
//     SubscriptionChannel<GovernanceContext>,
// );
pub type GovernanceEvent = PolicyFilterEvent<PlanningOutcome, GovernanceContext>;

#[tracing::instrument(level = "trace", skip(agent))]
pub async fn make_governance_phase<A>(
    _settings: &GovernanceSettings, agent: &mut A,
) -> Result<GovernancePhase>
where
    A: ClearinghouseSubscriptionAgent,
{
    let name = "governance";
    let context_channel =
        phases::subscribe_channel_with_agent(format!("{name}_context").as_str(), agent).await?;
    let governance = Box::new(GovernanceStage::new(name));
    (context_channel.outlet(), governance.context_inlet()).connect().await;
    Ok((governance, context_channel))
}
// {
//     let name = "governance";
//     let policy = GovernancePolicy::new(settings);
//     let subscription = policy.subscription(name, &settings.policy);
//     // todo: inject a "monitor relay" that captures gov's policy_filter events and passes along blocked,
//     // BUT holds Passed until data passes transform, when it sends the Passed event.
//     // May need to intro "builder" to policy phase to access filter's monitor to decorate.
//     let governance =
//         Box::new(PolicyPhase::with_transform(name, policy, make_governance_transform(name)).await?);
//     let channel = phases::subscribe_policy_phase(subscription, &governance, agent).await?;
//     Ok((governance, channel))
// }
