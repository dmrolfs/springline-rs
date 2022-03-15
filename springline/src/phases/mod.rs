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
