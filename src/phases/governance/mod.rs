pub mod context;
pub mod policy;
pub mod result;

use crate::phases::plan::{PlanningOutcome, ScalePlan};
use crate::Result;

pub use context::*;
pub use policy::*;
use proctor::elements::PolicySettings;
use proctor::graph::{Connect, SourceShape};
use proctor::phases::collection::{ClearinghouseSubscriptionMagnet, SubscriptionChannel};
use proctor::phases::policy_phase::PolicyPhase;
pub use result::*;

pub type GovernanceOutcome = PlanningOutcome;
pub type GovernanceApi = proctor::elements::PolicyFilterApi<GovernanceContext>;
pub type GovernanceMonitor = proctor::elements::PolicyFilterMonitor<ScalePlan, GovernanceContext>;
pub type GovernancePhase = Box<PolicyPhase<PlanningOutcome, GovernanceOutcome, GovernanceContext>>;

#[tracing::instrument(level = "info")]
pub async fn make_governance_phase(
    settings: &PolicySettings,
    clearinghouse_magnet: ClearinghouseSubscriptionMagnet<'_>,
) -> Result<GovernancePhase> {
    let name = "governance";
    let (policy, context_channel) =
        do_connect_governance_context(name, settings, clearinghouse_magnet).await?;

    let governance =
        PolicyPhase::with_transform(name, policy, make_governance_transform(name)).await;

    (context_channel.outlet(), governance.context_inlet())
        .connect()
        .await;
    let phase: GovernancePhase = Box::new(governance);
    Ok(phase)
}

#[tracing::instrument(level = "info", skip(policy_settings, magnet))]
async fn do_connect_governance_context(
    context_name: &str,
    policy_settings: &PolicySettings,
    magnet: ClearinghouseSubscriptionMagnet<'_>,
) -> Result<(GovernancePolicy, SubscriptionChannel<GovernanceContext>)> {
    let policy = GovernancePolicy::new(policy_settings);
    let channel = SubscriptionChannel::connect_channel(context_name, magnet).await?;
    Ok((policy, channel))
}
