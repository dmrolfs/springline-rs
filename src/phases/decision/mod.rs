pub mod context;
pub mod policy;
pub mod result;

use crate::phases::eligibility::EligibilityOutcome;
use crate::phases::MetricCatalog;
use crate::settings::Settings;
use crate::Result;

pub use context::*;
pub use policy::*;

use proctor::graph::stage::{ThroughStage, WithApi, WithMonitor};

use proctor::phases::collection::{ClearinghouseApi, SubscriptionChannel, ClearinghouseCmd};
use proctor::elements::PolicySettings;
use proctor::phases::decision::Decision;
use proctor::graph::{Connect, SinkShape, SourceShape, ThroughShape};
use result::{DecisionResult, make_decision_transform};


pub type DecisionOutcome = DecisionResult<MetricCatalog>;
pub type DecisionApi = proctor::elements::PolicyFilterApi<FlinkDecisionContext>;
pub type DecisionMonitor = proctor::elements::PolicyFilterMonitor<MetricCatalog, FlinkDecisionContext>;

pub struct DecisionConstituents {
    pub stage: Box<dyn ThroughStage<EligibilityOutcome, DecisionOutcome>>,
}

impl DecisionConstituents {
    pub fn new(stage: Box<dyn ThroughStage<EligibilityOutcome, DecisionOutcome>>) -> Self {
        Self { stage }
    }
}

impl WithApi for DecisionConstituents {
    type Sender = proctor::elements::PolicyFilterApi<FlinkDecisionContext>;

    fn tx_api(&self) -> Self::Sender {
        self.stage.tx_api()
    }
}

impl WithMonitor for DecisionConstituents {
    type Receiver = proctor::elements::PolicyFilterMonitor<MetricCatalog, FlinkDecisionContext>;

    fn rx_monitor(&self) -> Self::Receiver {
        self.stage.rx_monitor()
    }
}

#[tracing::instrument(level = "info", skip(settings, tx_clearinghouse_api))]
pub async fn make_decision_phase(
    settings: &Settings,
    tx_clearinghouse_api: &ClearinghouseApi,
) -> Result<DecisionConstituents> {
    let name = "decision";
    let (policy, context_channel) = do_connect_decision_context(name, &settings.decision_policy, tx_clearinghouse_api).await?;
    let decision = Decision::with_transform(
        name,
        policy,
        make_decision_transform(name)
    ).await;
    (context_channel.outlet(), decision.context_inlet()).connect().await;
    Ok(DecisionConstituents { stage: Box::new(decision), })
}

#[tracing::instrument(level="info", skip(tx_clearinghouse_api))]
async fn do_connect_decision_context(
    context_name: &str,
    policy_settings:& PolicySettings,
    tx_clearinghouse_api: &ClearinghouseApi,
) -> Result<(FlinkDecisionPolicy, SubscriptionChannel<FlinkDecisionContext>)> {
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
