pub mod context;
pub mod policy;
pub mod result;

use crate::phases::eligibility::EligibilityOutcome;
use crate::phases::MetricCatalog;
use crate::Result;

pub use context::*;
pub use policy::*;

use proctor::elements::PolicySettings;
use proctor::graph::{Connect, SourceShape};
use proctor::phases::collection::{ClearinghouseSubscriptionMagnet, SubscriptionChannel};
use proctor::phases::policy_phase::PolicyPhase;
use result::{make_decision_transform, DecisionResult};

pub type DecisionOutcome = DecisionResult<MetricCatalog>;
pub type DecisionApi = proctor::elements::PolicyFilterApi<DecisionContext>;
pub type DecisionMonitor = proctor::elements::PolicyFilterMonitor<MetricCatalog, DecisionContext>;

pub type DecisionPhase = Box<PolicyPhase<EligibilityOutcome, DecisionOutcome, DecisionContext>>;

#[tracing::instrument(level = "info")]
pub async fn make_decision_phase(
    settings: &PolicySettings,
    clearinghouse_magnet: ClearinghouseSubscriptionMagnet<'_>,
) -> Result<DecisionPhase> {
    let name = "decision";
    let (policy, context_channel) =
        do_connect_decision_context(name, settings, clearinghouse_magnet).await?;

    let decision = PolicyPhase::with_transform(name, policy, make_decision_transform(name)).await;

    (context_channel.outlet(), decision.context_inlet())
        .connect()
        .await;
    let phase: DecisionPhase = Box::new(decision);
    Ok(phase)
}

#[tracing::instrument(level = "info")]
async fn do_connect_decision_context(
    context_name: &str,
    policy_settings: &PolicySettings,
    magnet: ClearinghouseSubscriptionMagnet<'_>,
) -> Result<(DecisionPolicy, SubscriptionChannel<DecisionContext>)> {
    let policy = DecisionPolicy::new(policy_settings);
    let channel = SubscriptionChannel::connect_channel(context_name, magnet).await?;
    Ok((policy, channel))
}
