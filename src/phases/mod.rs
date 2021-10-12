use crate::Result;
// use fix_hidden_lifetime_bug;
pub use metric_catalog::*;
use proctor::elements::Telemetry;
use proctor::graph::{Connect, SourceShape};
use proctor::phases::collection::{ClearinghouseSubscriptionMagnet, SubscriptionChannel, TelemetrySubscription};
use proctor::phases::policy_phase::PolicyPhase;
use proctor::{AppData, ProctorContext, SharedString};

pub mod collection;
pub mod decision;
pub mod eligibility;
pub mod execution;
pub mod governance;
pub mod metric_catalog;
pub mod plan;

pub trait UpdateMetrics {
    fn update_metrics_for(name: SharedString) -> Box<dyn Fn(&str, &Telemetry) -> () + Send + Sync + 'static>;
}

// #[fix_hidden_lifetime_bug]
#[tracing::instrument(level = "info")]
pub async fn subscribe_policy_phase<In, Out, C>(
    subscription: TelemetrySubscription, phase: &Box<PolicyPhase<In, Out, C>>,
    magnet: ClearinghouseSubscriptionMagnet<'_>,
) -> Result<()>
where
    In: AppData + oso::ToPolar,
    Out: AppData,
    C: ProctorContext,
{
    let context_channel = SubscriptionChannel::connect_subscription(subscription, magnet).await?;
    (context_channel.outlet(), phase.context_inlet()).connect().await;
    Ok(())
}

// pub type PhaseStage<In, Out> = Box<dyn ThroughStage<In, Out>>;
//
// trait MyPolicy<T, C> {
//     // type Item = <Self as QueryPolicy>::Item;
//     // type Context = <Self as QueryPolicy>::Context;
//
//     type Subscription: PolicySubscription<Requirements = C>;
//     type Query: QueryPolicy<Context = C>;
//
//     fn for_settings(settings: &PolicySettings) -> Self;
// }
//
//
// #[tracing::instrument(level="info", skip(tx_clearinghouse_api))]
// pub async fn make_phase_stage<T, C, P, Out, F>(
//     phase_name: impl AsRef<str>,
//     policy_settings: &PolicySettings,
//     tx_clearinghouse_api: &ClearinghouseApi,
//     transform: Option<F>,
// ) -> Result<PhaseStage<T, Out>>
// where
//     P: MyPolicy<T, C>,
//     F: ThroughStage<PolicyOutcome<T, C>, Out>,
// {
//     let policy_context_channel = do_connect_context::<P>(
//         phase_name.as_ref(),
//         policy_settings,
//         tx_clearinghouse_api
//     ).await?;
//     let policy: P = policy_context_channel.0;
//     let context_channel: P::Context = policy_context_channel.1;
//
//     let phase = match transform {
//         Some(xform) => PolicyPhase::with_transform(phase_name.as_ref(), policy, xform).await,
//         None => PolicyPhase::carry_policy_outcome(phase_name.as_ref(), policy).await,
//     };
//
//     (context_channel.outlet(), phase.context_inlet()).connect().await;
//     let phase_stage: PhaseStage<P::Item, Out> = Box::new(phase);
//     Ok(phase_stage)
// }
//
// #[tracing::instrument(level="info", skip(phase_name, settings, tx_clearinghouse_api))]
// async fn do_connect_context<P>(
//     phase_name: &str,
//     settings: &PolicySettings,
//     tx_clearinghouse_api: &ClearinghouseApi
// ) -> Result<(P, SubscriptionChannel<P::Context>)>
// where
//     P: MyPolicy,
// {
//     let policy = P::for_settings(settings);
//     let channel = SubscriptionChannel::new(phase_name).await?;
//     let (subscribe_cmd, rx_subscribe_ack) = ClearinghouseCmd::subscribe(
//         policy.subscription(phase_name),
//         channel.subscription_receiver.clone(),
//     );
//     tx_clearinghouse_api.send(subscribe_cmd)?;
//     rx_subscribe_ack.await?;
//     Ok((policy, channel))
// }
