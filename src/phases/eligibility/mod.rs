use crate::phases::{self, MetricCatalog};
use crate::Result;
use proctor::elements::{PolicyFilterEvent, PolicySubscription};
use proctor::phases::collection::ClearinghouseSubscriptionMagnet;
use proctor::phases::policy_phase::PolicyPhase;
use proctor::SharedString;

use crate::settings::EligibilitySettings;
pub use context::{ClusterStatus, EligibilityContext, TaskStatus};
pub use policy::{EligibilityPolicy, EligibilityTemplateData};

pub mod context;
pub mod policy;

pub type EligibilityOutcome = MetricCatalog;
pub type EligibilityApi = proctor::elements::PolicyFilterApi<EligibilityContext, EligibilityTemplateData>;
pub type EligibilityMonitor = proctor::elements::PolicyFilterMonitor<MetricCatalog, EligibilityContext>;
pub type EligibilityPhase =
    Box<PolicyPhase<MetricCatalog, EligibilityOutcome, EligibilityContext, EligibilityTemplateData>>;
pub type EligibilityEvent = PolicyFilterEvent<MetricCatalog, EligibilityContext>;

#[tracing::instrument(level = "info")]
pub async fn make_eligibility_phase(
    settings: &EligibilitySettings, magnet: ClearinghouseSubscriptionMagnet<'_>,
) -> Result<EligibilityPhase> {
    let name: SharedString = "eligibility".into();
    let policy = EligibilityPolicy::new(&settings);
    let subscription = policy.subscription(name.as_ref());
    let eligibility = Box::new(PolicyPhase::strip_policy_outcome(name.as_ref(), policy).await?);

    phases::subscribe_policy_phase(subscription, &eligibility, magnet).await?;
    Ok(eligibility)
}

// #[tracing::instrument(level = "info")]
// pub async fn make_eligibility_phase(
//     settings: &PolicySettings,
//     magnet: ClearinghouseSubscriptionMagnet<'_>,
// ) -> Result<EligibilityPhase> {
//     let context_name: SharedString = "eligibility".into();
//
//     let policy = EligibilityPolicy::new(settings);
//
//     let eligibility = PolicyPhase::strip_policy_outcome(context_name.as_ref(), policy).await;
//     let eligibility: EligibilityPhase = Box::new(eligibility);
//
//     let subscription = TelemetrySubscription::new(context_name.as_ref())
//         .for_requirements::<EligibilityContext>()
//         .with_update_metrics_fn(context::update_eligibility_context_metrics(context_name));
//
//     let context_channel = SubscriptionChannel::connect_subscription(subscription, magnet).await?;
//     (context_channel.outlet(), eligibility.context_inlet()).connect().await;
//
//     Ok(eligibility)
// }

// #[tracing::instrument(level = "info")]
// pub async fn make_eligibility_phase(
//     settings: &PolicySettings, clearinghouse_magnet: ClearinghouseSubscriptionMagnet<'_>,
// ) -> Result<EligibilityPhase> {
//     let name: SharedString = "eligibility".into();
//
//     let (policy, context_channel) =
//         do_connect_eligibility_context(name.clone(), settings, clearinghouse_magnet).await?;
//     let eligibility = PolicyPhase::strip_policy_outcome(name.as_ref(), policy).await;
//
//     (context_channel.outlet(), eligibility.context_inlet()).connect().await;
//
//     let phase: EligibilityPhase = Box::new(eligibility);
//     Ok(phase)
// }
//
// #[tracing::instrument(level = "info")]
// async fn do_connect_eligibility_context(
//     context_name: SharedString, policy_settings: &PolicySettings, magnet: ClearinghouseSubscriptionMagnet<'_>,
// ) -> Result<(EligibilityPolicy, SubscriptionChannel<EligibilityContext>)> {
//     let policy = EligibilityPolicy::new(policy_settings);
//
//     let subscription = TelemetrySubscription::new(context_name.as_ref())
//         .for_requirements::<EligibilityContext>()
//         .with_update_metrics_fn(context::update_eligibility_context_metrics(context_name));
//
//     let channel = SubscriptionChannel::connect_subscription(subscription, magnet).await?;
//
//     Ok((policy, channel))
// }
