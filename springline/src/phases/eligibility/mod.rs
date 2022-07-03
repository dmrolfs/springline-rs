use proctor::elements::{PolicyFilterEvent, PolicySubscription};
use proctor::phases::policy_phase::PolicyPhase;
use proctor::phases::sense::{ClearinghouseSubscriptionAgent, SubscriptionChannel};
use proctor::SharedString;

use crate::phases;
use crate::settings::EligibilitySettings;
use crate::Result;

#[cfg(test)]
mod tests;

mod context;
mod policy;

pub use context::{ClusterStatus, EligibilityContext, TaskStatus};
pub use context::{CLUSTER__IS_RESCALING, CLUSTER__LAST_DEPLOYMENT};
pub(crate) use context::{
    ELIGIBILITY_CTX_ALL_SINKS_HEALTHY, ELIGIBILITY_CTX_CLUSTER_IS_DEPLOYING, ELIGIBILITY_CTX_CLUSTER_LAST_DEPLOYMENT,
    ELIGIBILITY_CTX_TASK_LAST_FAILURE,
};
pub(crate) use policy::ELIGIBILITY_POLICY_INELIGIBLE_DECISIONS_COUNT;
pub use policy::{EligibilityPolicy, EligibilityTemplateData};

use crate::flink::{AppDataWindow, MetricCatalog};

pub type EligibilityData = AppDataWindow<MetricCatalog>;
pub type EligibilityOutcome = EligibilityData;
pub type EligibilityApi = proctor::elements::PolicyFilterApi<EligibilityContext, EligibilityTemplateData>;
pub type EligibilityMonitor = proctor::elements::PolicyFilterMonitor<EligibilityData, EligibilityContext>;
pub type EligibilityPhase = (
    Box<PolicyPhase<EligibilityData, EligibilityOutcome, EligibilityContext, EligibilityTemplateData>>,
    SubscriptionChannel<EligibilityContext>,
);
pub type EligibilityEvent = PolicyFilterEvent<EligibilityData, EligibilityContext>;

#[tracing::instrument(level = "trace", skip(agent))]
pub async fn make_eligibility_phase<A>(settings: &EligibilitySettings, agent: &mut A) -> Result<EligibilityPhase>
where
    A: ClearinghouseSubscriptionAgent,
{
    let name: SharedString = "eligibility".into();
    let policy = EligibilityPolicy::new(settings);
    let subscription = policy.subscription(name.as_ref(), settings);
    let eligibility = Box::new(PolicyPhase::strip_policy_outcome(name.as_ref(), policy).await?);
    let channel = phases::subscribe_policy_phase(subscription, &eligibility, agent).await?;
    Ok((eligibility, channel))
}
