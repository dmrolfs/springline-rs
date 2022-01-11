pub mod context;
pub mod policy;
pub mod result;

pub use context::*;
pub use policy::*;
use proctor::elements::{PolicyFilterEvent, PolicySubscription};
use proctor::phases::collection::{ClearinghouseSubscriptionMagnet, SubscriptionChannel};
use proctor::phases::policy_phase::PolicyPhase;
use proctor::SharedString;
pub use result::*;

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

#[tracing::instrument(level = "info")]
pub async fn make_governance_phase(
    settings: &GovernanceSettings, magnet: ClearinghouseSubscriptionMagnet<'_>,
) -> Result<GovernancePhase> {
    let name: SharedString = "governance".into();
    let policy = GovernancePolicy::new(settings);
    let subscription = policy.subscription(name.as_ref(), &settings.policy);
    //todo: inject a "monitor relay" that captures gov's policy_filter events and passes along blocked,
    // BUT holds Passed until data passes transform, when it sends the Passed event.
    // May need to intro "builder" to policy phase to access pfilter's monitor to decorate.
    let governance = Box::new(
        PolicyPhase::with_transform(name.clone(), policy, make_governance_transform(name.into_owned())).await?,
    );

    let channel = phases::subscribe_policy_phase(subscription, &governance, magnet).await?;
    Ok((governance, channel))
}

// #[tracing::instrument(level = "info")]
// pub async fn make_governance_phase(
//     settings: &PolicySettings, clearinghouse_magnet: ClearinghouseSubscriptionMagnet<'_>,
// ) -> Result<GovernancePhase> {
//     let name: SharedString = "governance".into();
//     let (policy, context_channel) = do_connect_governance_context(name.clone(), settings,
// clearinghouse_magnet).await?;
//
//     let governance =
//         PolicyPhase::with_transform(name.clone(), policy,
// make_governance_transform(name.into_owned())).await;
//
//     (context_channel.outlet(), governance.context_inlet()).connect().await;
//     let phase: GovernancePhase = Box::new(governance);
//     Ok(phase)
// }
//
// #[tracing::instrument(level = "info", skip(policy_settings, magnet))]
// async fn do_connect_governance_context(
//     context_name: SharedString, policy_settings: &PolicySettings, magnet:
// ClearinghouseSubscriptionMagnet<'_>, ) -> Result<(GovernancePolicy,
// SubscriptionChannel<GovernanceContext>)> {     let policy =
// GovernancePolicy::new(policy_settings);
//
//     let subscription = TelemetrySubscription::new(context_name.as_ref())
//         .for_requirements::<GovernanceContext>()
//         .with_update_metrics_fn(context::update_governance_context_metrics(context_name));
//
//     let channel = SubscriptionChannel::connect_subscription(subscription, magnet).await?;
//
//     Ok((policy, channel))
// }
