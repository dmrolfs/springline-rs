use crate::phases::MetricCatalog;
use crate::settings::Settings;
use crate::Result;
use context::FlinkEligibilityContext;
use policy::EligibilityPolicy;
use proctor::elements::{PolicySettings, PolicySubscription};
use proctor::graph::stage::ThroughStage;
use proctor::graph::{Connect, SinkShape, SourceShape};
use proctor::phases::collection::{ClearinghouseApi, ClearinghouseCmd, SubscriptionChannel};
use proctor::phases::policy_phase::PolicyPhase;

pub mod context;
pub mod policy;

pub type EligibilityOutcome = MetricCatalog;
pub type EligibilityApi = proctor::elements::PolicyFilterApi<FlinkEligibilityContext>;
pub type EligibilityMonitor =
    proctor::elements::PolicyFilterMonitor<MetricCatalog, FlinkEligibilityContext>;
pub type EligibilityPhase = Box<dyn ThroughStage<MetricCatalog, EligibilityOutcome>>;

#[tracing::instrument(level = "info", skip(settings, tx_clearinghouse_api))]
pub async fn make_eligibility_phase(
    settings: &Settings,
    tx_clearinghouse_api: &ClearinghouseApi,
) -> Result<EligibilityPhase> {
    let name = "eligibility";

    let data_channel = MetricCatalog::connect_channel(name, tx_clearinghouse_api).await?;
    let (policy, context_channel) =
        do_connect_eligibility_context(name, &settings.eligibility_policy, tx_clearinghouse_api)
            .await?;
    let eligibility = PolicyPhase::strip_policy_outcome(name, policy).await;

    (context_channel.outlet(), eligibility.context_inlet())
        .connect()
        .await;
    (data_channel.outlet(), eligibility.inlet()).connect().await;

    let phase: EligibilityPhase = Box::new(eligibility);
    Ok(phase)
}

#[tracing::instrument(level = "info", skip(tx_clearinghouse_api))]
async fn do_connect_eligibility_context(
    context_name: &str,
    policy_settings: &PolicySettings,
    tx_clearinghouse_api: &ClearinghouseApi,
) -> Result<(
    EligibilityPolicy,
    SubscriptionChannel<FlinkEligibilityContext>,
)> {
    let policy = EligibilityPolicy::new(policy_settings);
    let channel = SubscriptionChannel::new(context_name).await?;
    let (subscribe_cmd, rx_subscribe_ack) = ClearinghouseCmd::subscribe(
        policy.subscription(context_name),
        channel.subscription_receiver.clone(),
    );
    tx_clearinghouse_api.send(subscribe_cmd)?;
    rx_subscribe_ack.await?;
    Ok((policy, channel))
}

// pub mod policy {
// use crate::elements::{Policy, PolicySettings};
// use crate::flink::eligibility::context::{ClusterStatus, FlinkEligibilityContext, TaskStatus};
// use crate::flink::MetricCatalog;
// use oso::{Oso, PolarClass};
//
// pub fn make_eligibility_policy(
//     settings: &impl PolicySettings,
// ) -> impl Policy<MetricCatalog, FlinkEligibilityContext> {
//     let init = |engine: &mut Oso| {
//         engine.register_class(FlinkEligibilityContext::get_polar_class())?;
//         engine.register_class(
//             TaskStatus::get_polar_class_builder()
//                 .name("TaskStatus")
//                 .add_method("last_failure_within_seconds",
// TaskStatus::last_failure_within_seconds)                 .build(),
//         )?;
//         engine.register_class(
//             ClusterStatus::get_polar_class_builder()
//                 .name("ClusterStatus")
//                 .add_method(
//                     "last_deployment_within_seconds",
//                     ClusterStatus::last_deployment_within_seconds,
//                 )
//                 .build(),
//         )?;
//
//         Ok(())
//     };
//
//     make_item_context_policy("eligible", settings, init)
// }
// }
