use std::marker::PhantomData;
use std::time::Duration;

use async_trait::async_trait;
use proctor::AppData;
use tokio::time::Instant;

use super::{ActionSession, ScaleAction};
use crate::flink::FlinkContext;
use crate::math;
use crate::phases::act;
use crate::phases::act::{ActError, ActErrorDisposition};
use crate::phases::plan::ScaleActionPlan;

pub const ACTION_LABEL: &str = "flink_settlement";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlinkSettlement<P> {
    label: String,
    pub taskmanager_register_timeout: Duration,
    pub flink_polling_interval: Duration,
    marker: PhantomData<P>,
}

impl<P> FlinkSettlement<P> {
    pub fn from_settings(
        taskmanager_register_timeout: Duration, flink_polling_interval: Duration,
    ) -> Self {
        Self {
            label: ACTION_LABEL.to_string(),
            taskmanager_register_timeout,
            flink_polling_interval,
            marker: PhantomData,
        }
    }

    pub fn with_sub_label(&self, sub_label: impl AsRef<str>) -> Self {
        Self {
            label: format!("{}::{}", self.label, sub_label.as_ref()),
            taskmanager_register_timeout: self.taskmanager_register_timeout,
            flink_polling_interval: self.flink_polling_interval,
            marker: PhantomData,
        }
    }
}

#[async_trait]
impl<P> ScaleAction for FlinkSettlement<P>
where
    P: AppData + ScaleActionPlan,
{
    type Plan = P;

    fn label(&self) -> &str {
        self.label.as_str()
    }

    fn check_preconditions(&self, _session: &ActionSession) -> Result<(), ActError> {
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "FlinkSettlement::execute", skip(self, plan,), fields(label=%self.label()))]
    async fn execute<'s>(
        &mut self, plan: &'s Self::Plan, session: &'s mut ActionSession,
    ) -> Result<(), ActError> {
        let confirmed_nr_taskmanagers =
            self.block_for_rescaled_taskmanagers(plan, &session.flink).await;
        session.nr_confirmed_rescaled_taskmanagers = Some(confirmed_nr_taskmanagers);

        Ok(())
    }
}

impl<P> FlinkSettlement<P>
where
    P: AppData + ScaleActionPlan,
{
    #[tracing::instrument(
    level = "info",
    name = "flink_settlement::block_for_rescaled_taskmanagers"
        skip(self, plan, flink),
        fields(label=%self.label()),
    )]
    async fn block_for_rescaled_taskmanagers(
        &self, plan: &<Self as ScaleAction>::Plan, flink: &FlinkContext,
    ) -> u32 {
        let correlation = plan.correlation();
        let nr_target_taskmanagers = plan.target_replicas();

        tracing::debug!(
            ?correlation,
            "budgeting {:?} for Flink rescaled taskmanagers to register with job manager.",
            self.taskmanager_register_timeout
        );

        let mut nr_confirmed_taskmanagers = 0;
        let start = Instant::now();
        while Instant::now().duration_since(start) < self.taskmanager_register_timeout {
            match flink.query_taskmanagers(correlation).await {
                Ok(tm_detail) => {
                    nr_confirmed_taskmanagers =
                        math::saturating_usize_to_u32(tm_detail.nr_taskmanagers);
                    if nr_confirmed_taskmanagers == nr_target_taskmanagers {
                        tracing::info!(
                            %nr_confirmed_taskmanagers, %nr_target_taskmanagers, ?correlation,
                            "confirmed rescaled taskmanagers connected with Flink JobManager"
                        );
                        break;
                    }
                },
                Err(err) => {
                    let track = format!("{}::query_taskmanagers", self.label());
                    tracing::warn!(
                        error=?err, ?plan, ?correlation, %track,
                        "failed to query number of taskmanagers - retrying in {:?}", self.flink_polling_interval
                    );
                    act::track_act_errors(&track, Some(&err), ActErrorDisposition::Ignored, plan);
                    break;
                },
            }

            let budget_remaining =
                self.taskmanager_register_timeout - Instant::now().duration_since(start);
            tracing::info!(
                ?correlation,
                "flink_settlement: flink confirmed {nr_confirmed_taskmanagers} taskmanagers registered \
                of {nr_target_taskmanagers} requested - budget remaining: {budget_remaining:?}, \
                checking again in {:?}",
                self.flink_polling_interval
            );

            tokio::time::sleep(self.flink_polling_interval).await;
        }

        if nr_confirmed_taskmanagers != nr_target_taskmanagers {
            tracing::error!(
                %nr_confirmed_taskmanagers, %nr_target_taskmanagers, ?correlation,
                "could not confirm rescaled taskmanager connected with Flink JobManager within {:?} settle timeout - continuing with next action.",
                self.taskmanager_register_timeout
            );
        }

        nr_confirmed_taskmanagers
    }
}
