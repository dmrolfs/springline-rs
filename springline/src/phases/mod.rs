use pretty_snowflake::Label;
use proctor::elements::QueryResult;
use proctor::graph::{Connect, SourceShape};
use proctor::phases::policy_phase::PolicyPhase;
use proctor::phases::sense::{
    ClearinghouseSubscriptionAgent, SubscriptionChannel, SubscriptionRequirements,
    TelemetrySubscription,
};
use proctor::{AppData, Correlation, ProctorContext};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::{Env, Result};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod policy_test_fixtures;

pub mod act;
mod collect_window;
pub mod decision;
pub mod eligibility;
pub mod governance;
pub mod plan;
pub mod sense;

use crate::flink::{AppDataWindow, MetricCatalog};
pub use collect_window::{CollectMetricWindow, WindowApi, WindowCmd};

pub type PhaseDataT = MetricCatalog;
pub type PhaseData = Env<PhaseDataT>;
pub type WindowDataT = AppDataWindow<PhaseData>;
pub type WindowData = Env<WindowDataT>;

pub const REASON: &str = "reason";
pub const UNSPECIFIED: &str = "unspecified";

pub fn get_outcome_reason(results: &QueryResult) -> String {
    results
        .bindings
        .get(REASON)
        .and_then(|rs| rs.first())
        .map(|r| r.to_string())
        .unwrap_or_else(|| UNSPECIFIED.to_string())
}

#[tracing::instrument(level = "trace", skip(agent))]
pub(crate) async fn subscribe_policy_phase<In, Out, C, D, A>(
    subscription: TelemetrySubscription, phase: &PolicyPhase<In, Out, Env<C>, D>, agent: &mut A,
) -> Result<SubscriptionChannel<C>>
where
    In: AppData + Correlation + oso::ToPolar,
    Out: AppData,
    C: ProctorContext + Label + PartialEq,
    Env<C>: ProctorContext,
    D: AppData + Serialize,
    A: ClearinghouseSubscriptionAgent,
{
    let context_channel = SubscriptionChannel::connect_subscription(subscription, agent).await?;
    (context_channel.outlet(), phase.context_inlet()).connect().await;
    Ok(context_channel)
}

#[tracing::instrument(level = "trace", skip(agent))]
pub(crate) async fn subscribe_channel_with_agent<T, A>(
    name: &str, agent: &mut A,
) -> Result<SubscriptionChannel<T>>
where
    T: AppData + Label + SubscriptionRequirements + DeserializeOwned,
    A: ClearinghouseSubscriptionAgent,
{
    let subscription = TelemetrySubscription::new(name).for_requirements::<T>();
    let channel = SubscriptionChannel::new(name).await?;
    agent
        .subscribe(subscription, channel.subscription_receiver.clone())
        .await?;
    Ok(channel)
}
