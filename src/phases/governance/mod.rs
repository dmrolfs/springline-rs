pub mod context;
pub mod policy;
pub mod result;

use crate::phases::plan::{PlanningOutcome, ScalePlan};
use crate::settings::Settings;
use crate::Result;

pub use context::*;
pub use policy::*;
use proctor::elements::{PolicySettings, PolicySubscription};
use proctor::graph::{Connect, SourceShape};
use proctor::phases::collection::{ClearinghouseApi, ClearinghouseCmd, SubscriptionChannel};
use proctor::phases::policy_phase::PolicyPhase;
pub use result::*;

pub type GovernanceOutcome = PlanningOutcome;
pub type GovernanceApi = proctor::elements::PolicyFilterApi<GovernanceContext>;
pub type GovernanceMonitor = proctor::elements::PolicyFilterMonitor<ScalePlan, GovernanceContext>;
pub type GovernancePhase = Box<PolicyPhase<PlanningOutcome, GovernanceOutcome, GovernanceContext>>;

#[tracing::instrument(level = "info", skip(settings, tx_clearinghouse_api))]
pub async fn make_governance_phase(
    settings: &Settings,
    tx_clearinghouse_api: &ClearinghouseApi,
) -> Result<GovernancePhase> {
    let name = "governance";
    let (policy, context_channel) =
        do_connect_governance_context(name, &settings.governance, tx_clearinghouse_api).await?;

    let governance =
        PolicyPhase::with_transform(name, policy, make_governance_transform(name)).await;

    (context_channel.outlet(), governance.context_inlet())
        .connect()
        .await;
    let phase: GovernancePhase = Box::new(governance);
    Ok(phase)
}

#[tracing::instrument(level = "info", skip(tx_clearinghouse_api))]
async fn do_connect_governance_context(
    context_name: &str,
    policy_settings: &PolicySettings,
    tx_clearinghouse_api: &ClearinghouseApi,
) -> Result<(GovernancePolicy, SubscriptionChannel<GovernanceContext>)> {
    let policy = GovernancePolicy::new(policy_settings);
    let channel = SubscriptionChannel::new(context_name).await?;
    let (subscribe_cmd, rx_subscribe_ack) = ClearinghouseCmd::subscribe(
        policy.subscription(context_name),
        channel.subscription_receiver.clone(),
    );
    tx_clearinghouse_api.send(subscribe_cmd)?;
    rx_subscribe_ack.await?;
    Ok((policy, channel))
}
