use proctor::elements::{PolicyFilterEvent, PolicySubscription};
use proctor::phases::policy_phase::PolicyPhase;
use proctor::phases::sense::{ClearinghouseSubscriptionAgent, SubscriptionChannel};
use proctor::SharedString;

use crate::settings::EligibilitySettings;
use crate::Result;
use crate::{phases, Env};

#[cfg(test)]
mod policy_tests;
#[cfg(test)]
mod tests;

mod context;
mod policy;

pub use context::{ClusterStatus, EligibilityContext, JobStatus};
pub use context::{CLUSTER__IS_RESCALING, CLUSTER__LAST_DEPLOYMENT};
pub(crate) use context::{
    ELIGIBILITY_CTX_ALL_SINKS_HEALTHY, ELIGIBILITY_CTX_CLUSTER_IS_DEPLOYING,
    ELIGIBILITY_CTX_CLUSTER_LAST_DEPLOYMENT, ELIGIBILITY_CTX_TASK_LAST_FAILURE,
};
pub(crate) use policy::ELIGIBILITY_POLICY_INELIGIBLE_DECISIONS_COUNT;
pub use policy::{EligibilityPolicy, EligibilityTemplateData};

use crate::flink::{AppDataWindow, MetricCatalog};

pub type EligibilityDataT = AppDataWindow<Env<MetricCatalog>>;
pub type EligibilityData = Env<EligibilityDataT>;
pub type EligibilityOutcome = Env<EligibilityDataT>;
pub type EligibilityApi =
    proctor::elements::PolicyFilterApi<Env<EligibilityContext>, EligibilityTemplateData>;
pub type EligibilityMonitor =
    proctor::elements::PolicyFilterMonitor<EligibilityData, Env<EligibilityContext>>;
pub type EligibilityPhase = (
    Box<
        PolicyPhase<
            EligibilityData,
            EligibilityOutcome,
            Env<EligibilityContext>,
            EligibilityTemplateData,
        >,
    >,
    SubscriptionChannel<EligibilityContext>,
);
pub type EligibilityEvent = PolicyFilterEvent<EligibilityData, Env<EligibilityContext>>;

#[tracing::instrument(level = "trace", skip(agent))]
pub async fn make_eligibility_phase<A>(
    settings: &EligibilitySettings, agent: &mut A,
) -> Result<EligibilityPhase>
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
