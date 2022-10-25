mod context;
use crate::phases::plan::PlanningOutcomeT;
use crate::phases::{self, plan::PlanningOutcome};
use crate::settings::GovernanceSettings;
use crate::{Env, Result};
pub use context::GovernanceContext;
use proctor::elements::PolicyFilterEvent;
use proctor::graph::{Connect, SourceShape};
use proctor::phases::sense::{ClearinghouseSubscriptionAgent, SubscriptionChannel};

mod policy;
mod result;
mod stage;

pub use stage::GovernanceStage;

pub type GovernanceOutcome = PlanningOutcome;

pub type GovernanceMonitor<T> =
    proctor::elements::PolicyFilterMonitor<Env<T>, Env<GovernanceContext>>;
pub type GovernancePhase = (
    Box<GovernanceStage<PlanningOutcomeT>>,
    SubscriptionChannel<GovernanceContext>,
);
pub type GovernanceEvent<T> = PolicyFilterEvent<Env<T>, Env<GovernanceContext>>;

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
