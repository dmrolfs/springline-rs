use crate::phases::MetricCatalog;
use crate::Result;
use context::EligibilityContext;
use policy::EligibilityPolicy;
use proctor::elements::PolicySettings;
use proctor::graph::{Connect, SourceShape};
use proctor::phases::collection::{ClearinghouseSubscriptionMagnet, SubscriptionChannel};
use proctor::phases::policy_phase::PolicyPhase;

pub mod context;
pub mod policy;

pub type EligibilityOutcome = MetricCatalog;
pub type EligibilityApi = proctor::elements::PolicyFilterApi<EligibilityContext>;
pub type EligibilityMonitor = proctor::elements::PolicyFilterMonitor<MetricCatalog, EligibilityContext>;
pub type EligibilityPhase = Box<PolicyPhase<MetricCatalog, EligibilityOutcome, EligibilityContext>>;

#[tracing::instrument(level = "info")]
pub async fn make_eligibility_phase(
    settings: &PolicySettings, clearinghouse_magnet: ClearinghouseSubscriptionMagnet<'_>,
) -> Result<EligibilityPhase> {
    let name = "eligibility";

    let (policy, context_channel) = do_connect_eligibility_context(name, settings, clearinghouse_magnet).await?;
    let eligibility = PolicyPhase::strip_policy_outcome(name, policy).await;

    (context_channel.outlet(), eligibility.context_inlet()).connect().await;

    let phase: EligibilityPhase = Box::new(eligibility);
    Ok(phase)
}

#[tracing::instrument(level = "info")]
async fn do_connect_eligibility_context<'c>(
    context_name: &str, policy_settings: &PolicySettings, clearinghouse_magnet: ClearinghouseSubscriptionMagnet<'c>,
) -> Result<(EligibilityPolicy, SubscriptionChannel<EligibilityContext>)> {
    let policy = EligibilityPolicy::new(policy_settings);

    let channel: SubscriptionChannel<EligibilityContext> =
        SubscriptionChannel::connect_channel(context_name, clearinghouse_magnet).await?;

    Ok((policy, channel))
}
