pub mod context;
pub mod policy;
pub mod result;

use crate::phases::eligibility::EligibilityOutcome;
use crate::phases::MetricCatalog;
use crate::settings::Settings;
use crate::Result;

pub use context::*;
pub use policy::*;

use proctor::graph::stage::ThroughStage;

use proctor::elements::{PolicySettings, PolicySubscription};
use proctor::graph::{Connect, SourceShape};
use proctor::phases::collection::{ClearinghouseApi, ClearinghouseCmd, SubscriptionChannel};
use proctor::phases::policy_phase::PolicyPhase;
use result::{make_decision_transform, DecisionResult};

pub type DecisionOutcome = DecisionResult<MetricCatalog>;
pub type DecisionApi = proctor::elements::PolicyFilterApi<FlinkDecisionContext>;
pub type DecisionMonitor =
    proctor::elements::PolicyFilterMonitor<MetricCatalog, FlinkDecisionContext>;

pub type DecisionPhase = Box<dyn ThroughStage<EligibilityOutcome, DecisionOutcome>>;

#[tracing::instrument(level = "info", skip(settings, tx_clearinghouse_api))]
pub async fn make_decision_phase(
    settings: &Settings,
    tx_clearinghouse_api: &ClearinghouseApi,
) -> Result<DecisionPhase> {
    let name = "decision";
    let (policy, context_channel) =
        do_connect_decision_context(name, &settings.decision_policy, tx_clearinghouse_api).await?;

    let decision = PolicyPhase::with_transform(name, policy, make_decision_transform(name)).await;

    (context_channel.outlet(), decision.context_inlet())
        .connect()
        .await;
    let stage: DecisionPhase = Box::new(decision);
    Ok(stage)
}

#[tracing::instrument(level = "info", skip(tx_clearinghouse_api))]
async fn do_connect_decision_context(
    context_name: &str,
    policy_settings: &PolicySettings,
    tx_clearinghouse_api: &ClearinghouseApi,
) -> Result<(
    FlinkDecisionPolicy,
    SubscriptionChannel<FlinkDecisionContext>,
)> {
    let policy = FlinkDecisionPolicy::new(policy_settings);
    let channel = SubscriptionChannel::new(context_name).await?;
    let (subscribe_cmd, rx_subscribe_ack) = ClearinghouseCmd::subscribe(
        policy.subscription(context_name),
        channel.subscription_receiver.clone(),
    );
    tx_clearinghouse_api.send(subscribe_cmd)?;
    rx_subscribe_ack.await?;
    Ok((policy, channel))
}
