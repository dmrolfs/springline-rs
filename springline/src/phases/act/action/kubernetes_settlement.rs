use std::marker::PhantomData;
use std::time::Duration;

use async_trait::async_trait;
use proctor::AppData;
use tokio::time::Instant;

use super::{ActionSession, ScaleAction};
use crate::kubernetes::{FlinkComponent, KubernetesContext, RUNNING_STATUS};
use crate::math;
use crate::phases::act;
use crate::phases::act::{ActError, ActErrorDisposition};
use crate::phases::plan::ScaleActionPlan;

pub const ACTION_LABEL: &str = "kubernetes_settlement";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KubernetesSettlement<P> {
    label: String,
    timeout: Duration,
    polling_interval: Duration,
    marker: PhantomData<P>,
}

impl<P> KubernetesSettlement<P> {
    pub fn from_settings(timeout: Duration, polling_interval: Duration) -> Self {
        Self {
            label: ACTION_LABEL.to_string(),
            timeout,
            polling_interval,
            marker: PhantomData,
        }
    }

    pub fn with_sub_label(&self, sub_label: impl AsRef<str>) -> Self {
        Self {
            label: format!("{}::{}", self.label, sub_label.as_ref()),
            timeout: self.timeout,
            polling_interval: self.polling_interval,
            marker: PhantomData,
        }
    }
}

#[async_trait]
impl<P> ScaleAction for KubernetesSettlement<P>
where
    P: AppData + ScaleActionPlan,
{
    type Plan = P;

    fn label(&self) -> &str {
        self.label.as_str()
    }

    fn check_preconditions(&self, session: &ActionSession) -> Result<(), ActError> {
        session
            .nr_target_replicas
            .map(|_| ())
            .ok_or_else(|| ActError::ActionPrecondition {
                action: self.label().to_string(),
                reason: "nr_target_replicas unset in action session".to_string(),
            })
    }

    #[tracing::instrument(
        level = "info",
        name = "KubernetesSettlement::execute",
        skip(self, plan,),
        fields(label=%self.label()),
    )]
    async fn execute<'s>(
        &mut self, plan: &'s Self::Plan, session: &'s mut ActionSession,
    ) -> Result<(), ActError> {
        let nr_confirmed_taskmanagers = self.block_for_rescaled_taskmanagers(plan, session).await;
        let nr_target_taskmanagers = Self::target_replicas(plan, session);

        if nr_confirmed_taskmanagers != nr_target_taskmanagers {
            let track = format!("{}::settlement_confirmation", self.label());
            tracing::error!(
                %nr_confirmed_taskmanagers, %nr_target_taskmanagers, coreelation=?session.correlation, %track,
                "{}: could not confirm rescaled taskmanager pods {RUNNING_STATUS} in kubernetes within {:?} settle timeout - continuing with next action.",
                self.label(), self.timeout
            );
            act::track_act_errors::<ActError, _>(&track, None, ActErrorDisposition::Ignored, plan);
        }

        session.nr_target_replicas = None; //reset session value
        Ok(())
    }
}

impl<P> KubernetesSettlement<P>
where
    P: AppData + ScaleActionPlan,
{
    fn target_replicas<'s>(
        plan: &'s <Self as ScaleAction>::Plan, session: &'s ActionSession,
    ) -> u32 {
        session.nr_target_replicas.unwrap_or_else(|| plan.target_replicas())
    }

    #[tracing::instrument(
        level = "info",
        name = "KubernetesSettlement::block_for_rescaled_taskmanagers",
        skip(self, plan, session),
        fields(label=%self.label()),
    )]
    async fn block_for_rescaled_taskmanagers(
        &self, plan: &<Self as ScaleAction>::Plan, session: &ActionSession,
    ) -> u32 {
        let correlation = plan.correlation();
        let nr_target_taskmanagers = Self::target_replicas(plan, session);

        tracing::debug!(
            ?correlation,
            session_target=?session.nr_target_replicas, plan_target=%plan.target_replicas(),
            "budgeting {:?} for rescaling taskmanagers to {nr_target_taskmanagers} replicas to register with kubernetes.",
            self.timeout
        );

        let mut nr_confirmed_taskmanagers = 0;
        tokio::time::sleep(self.polling_interval).await;
        let start = Instant::now();
        while Instant::now().duration_since(start) < self.timeout {
            match session.kube.list_pods(FlinkComponent::TaskManager).await {
                Ok(tm_pods) => {
                    let tms_by_status = KubernetesContext::group_pods_by_status(&tm_pods);
                    nr_confirmed_taskmanagers = tms_by_status
                        .get(RUNNING_STATUS)
                        .map(|tms| math::saturating_usize_to_u32(tms.len()))
                        .unwrap_or(0);

                    if nr_confirmed_taskmanagers == nr_target_taskmanagers {
                        tracing::info!(
                            %nr_confirmed_taskmanagers, %nr_target_taskmanagers, ?correlation,
                            "confirmed rescaled taskmanagers {RUNNING_STATUS} in kubernetes."
                        );
                        break;
                    }
                },
                Err(err) => {
                    let track = format!("{}::list_pods", self.label());
                    tracing::warn!(
                        error=?err, ?plan, ?correlation, %track,
                        "failed to list taskmanager pods in kubernetes - retrying in {:?}", self.polling_interval
                    );
                    act::track_act_errors(&track, Some(&err), ActErrorDisposition::Ignored, plan);
                    break;
                },
            }

            let budget_remaining = self.timeout - Instant::now().duration_since(start);
            tracing::info!(
                ?correlation,
                "{}: kubernetes confirmed {nr_confirmed_taskmanagers} taskmanager pods registered \
                of {nr_target_taskmanagers} requested - budget remaining: {budget_remaining:?}, \
                checking again in {:?}",
                self.label(),
                self.polling_interval
            );

            tokio::time::sleep(self.polling_interval).await;
        }

        nr_confirmed_taskmanagers
    }
}
