// use fix_hidden_lifetime_bug;
use proctor::graph::{Connect, SourceShape};
use proctor::phases::policy_phase::PolicyPhase;
use proctor::phases::sense::{ClearinghouseSubscriptionAgent, SubscriptionChannel, TelemetrySubscription};
use proctor::{AppData, ProctorContext};
use serde::Serialize;

use crate::Result;

pub mod act;
pub mod decision;
pub mod eligibility;
pub mod governance;
pub mod plan;
pub mod sense;

pub const REASON: &str = "reason";

#[tracing::instrument(level = "trace", skip(agent))]
pub async fn subscribe_policy_phase<In, Out, C, D, A>(
    subscription: TelemetrySubscription, phase: &PolicyPhase<In, Out, C, D>, agent: &mut A,
) -> Result<SubscriptionChannel<C>>
where
    In: AppData + oso::ToPolar,
    Out: AppData,
    C: ProctorContext,
    D: AppData + Serialize,
    A: ClearinghouseSubscriptionAgent,
{
    let context_channel = SubscriptionChannel::connect_subscription(subscription, agent).await?;
    (context_channel.outlet(), phase.context_inlet()).connect().await;
    Ok(context_channel)
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
