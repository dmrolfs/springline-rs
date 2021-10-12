pub mod context;
pub mod policy;
pub mod result;

use crate::phases::eligibility::EligibilityOutcome;
use crate::phases::{self, MetricCatalog};
use crate::Result;

pub use context::*;
pub use policy::*;

use proctor::elements::{PolicySettings, PolicySubscription};
use proctor::phases::collection::ClearinghouseSubscriptionMagnet;
use proctor::phases::policy_phase::PolicyPhase;
use proctor::SharedString;
use result::{make_decision_transform, DecisionResult};

pub type DecisionOutcome = DecisionResult<MetricCatalog>;
pub type DecisionApi = proctor::elements::PolicyFilterApi<DecisionContext>;
pub type DecisionMonitor = proctor::elements::PolicyFilterMonitor<MetricCatalog, DecisionContext>;

pub type DecisionPhase = Box<PolicyPhase<EligibilityOutcome, DecisionOutcome, DecisionContext>>;

#[tracing::instrument(level = "info")]
pub async fn make_decision_phase(
    settings: &PolicySettings, magnet: ClearinghouseSubscriptionMagnet<'_>,
) -> Result<DecisionPhase> {
    let name: SharedString = "decision".into();
    let policy = DecisionPolicy::new(&settings);
    let subscription = policy.subscription(name.as_ref());
    let decision =
        Box::new(PolicyPhase::with_transform(name.clone(), policy, make_decision_transform(name.into_owned())).await);

    phases::subscribe_policy_phase(subscription, &decision, magnet).await?;
    Ok(decision)
}

// #[tracing::instrument(level = "info")]
// pub async fn make_decision_phase(
//     settings: &PolicySettings, clearinghouse_magnet: ClearinghouseSubscriptionMagnet<'_>,
// ) -> Result<DecisionPhase> {
//     let name: SharedString = "decision".into();
//     // let (policy, context_channel) = do_connect_decision_context(name.clone(), settings, clearinghouse_magnet).await?;
//     let policy = DecisionPolicy::new(policy_settings);
//
//     let decision = PolicyPhase::with_transform(name.clone(), policy, make_decision_transform(name.into_owned())).await;
//     let decision: DecisionPhase = Box::new(decision);
//
//     let subscription = TelemetrySubscription::new(context_name.as_ref())
//         .for_requirements::<DecisionContext>()
//         .with_update_metrics_fn(context::update_decision_context_metrics(context_name));
//
//     let channel = SubscriptionChannel::connect_subscription(subscription, magnet).await?;
//
//     (context_channel.outlet(), decision.context_inlet()).connect().await;
//     Ok(phase)
// }
//
// #[tracing::instrument(level = "info")]
// pub async fn make_decision_phase(
//     settings: &PolicySettings, clearinghouse_magnet: ClearinghouseSubscriptionMagnet<'_>,
// ) -> Result<DecisionPhase> {
//     let name: SharedString = "decision".into();
//     let (policy, context_channel) = do_connect_decision_context(name.clone(), settings, clearinghouse_magnet).await?;
//
//     let decision = PolicyPhase::with_transform(name.clone(), policy, make_decision_transform(name.into_owned())).await;
//
//     (context_channel.outlet(), decision.context_inlet()).connect().await;
//     let phase: DecisionPhase = Box::new(decision);
//     Ok(phase)
// }

// #[tracing::instrument(level = "info")]
// async fn do_connect_decision_context(
//     context_name: SharedString, policy_settings: &PolicySettings, magnet: ClearinghouseSubscriptionMagnet<'_>,
// ) -> Result<(DecisionPolicy, SubscriptionChannel<DecisionContext>)> {
//     let policy = DecisionPolicy::new(policy_settings);
//
//     let subscription = TelemetrySubscription::new(context_name.as_ref())
//         .for_requirements::<DecisionContext>()
//         .with_update_metrics_fn(context::update_decision_context_metrics(context_name));
//
//     let channel = SubscriptionChannel::connect_subscription(subscription, magnet).await?;
//
//     Ok((policy, channel))
// }
