use std::marker::PhantomData;
use std::time::Duration;

use async_trait::async_trait;
use proctor::AppData;
use tokio::time::Instant;

use super::{ActionSession, ScaleAction};
use crate::flink::FlinkContext;
use crate::phases::act;
use crate::phases::act::{ActError, ActErrorDisposition, ScaleActionPlan};

pub const ACTION_LABEL: &str = "settle_rescaled_replicas";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettleRescaledReplicas<P> {
    label: String,
    pub taskmanager_register_timeout: Duration,
    pub flink_polling_interval: Duration,
    marker: PhantomData<P>,
}

impl<P> SettleRescaledReplicas<P> {
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
impl<P> ScaleAction for SettleRescaledReplicas<P>
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

    #[tracing::instrument(level = "info", name = "SettleRescaledReplicas::execute", skip(self))]
    async fn execute<'s>(
        &mut self, plan: &'s Self::Plan, session: &'s mut ActionSession,
    ) -> Result<(), ActError> {
        let confirmed_nr_taskmanagers =
            self.block_for_rescaled_taskmanagers(plan, &session.flink).await;
        session.nr_confirmed_rescaled_taskmanagers = Some(confirmed_nr_taskmanagers);

        Ok(())
    }
}

impl<P> SettleRescaledReplicas<P>
where
    P: AppData + ScaleActionPlan,
{
    // #[tracing::instrument(
    //     level = "info",
    //     name = "patch_replicas::block_until_satisfied",
    //     skip(plan, kube)
    // )]
    // async fn block_until_satisfied(
    //     &self, plan: &<Self as ScaleAction>::Plan, kube: &KubernetesContext,
    // ) -> Result<(), ActError> {
    //     let correlation = plan.correlation();
    //     let target_nr_task_managers = plan.target_replicas();
    //     let mut pods_by_status = None;
    //     // TODO: convert to kube::runtime watcher - see pod watch example.
    //
    //     let api_constraints = kube.api_constraints();
    //     tracing::debug!(
    //         ?correlation,
    //         "budgeting {:?} to kubernetes for patch replicas with {:?} polling interval.",
    //         api_constraints.api_timeout,
    //         api_constraints.polling_interval
    //     );
    //
    //     let mut action_satisfied = false;
    //     let start = Instant::now();
    //     while start.elapsed() < api_constraints.api_timeout {
    //         let (statuses, is_satisfied) =
    //             Self::do_assess_patch_completion(plan, kube, start).await?;
    //         pods_by_status.replace(statuses);
    //         action_satisfied = is_satisfied;
    //         if action_satisfied {
    //             break;
    //         }
    //         tokio::time::sleep(api_constraints.polling_interval).await;
    //     }
    //
    //     if !action_satisfied {
    //         let status_counts: HashMap<String, usize> = pods_by_status
    //             .unwrap_or_default()
    //             .into_iter()
    //             .map(|(status, pods)| (status, pods.len()))
    //             .collect();
    //
    //         return Err(ActError::Timeout(
    //             kube.api_constraints().api_timeout,
    //             format!(
    //                 "Timed out waiting for patch replicas to complete: nr_target={} :: status_counts={:?} \
    //                  [correlation={}]",
    //                 target_nr_task_managers, status_counts, correlation,
    //             ),
    //         ));
    //     }
    //
    //     Ok(())
    // }
    //
    // #[tracing::instrument(
    //     level = "trace",
    //     name = "patch_replicas::assess_patch_completion",
    //     skip()
    // )]
    // async fn do_assess_patch_completion(
    //     plan: &<Self as ScaleAction>::Plan, kube: &KubernetesContext, start: Instant,
    // ) -> Result<(HashMap<String, Vec<Pod>>, bool), KubernetesError> {
    //     let correlation = plan.correlation();
    //     let target_nr_task_managers = plan.target_replicas();
    //     let kube_constraints = kube.api_constraints();
    //     let task_managers = kube.list_pods(FlinkComponent::TaskManager).await?;
    //     let pods_by_status = Self::group_pods_by_status(&task_managers);
    //
    //     let (nr_running, pod_status_counts) = Self::do_count_running_pods(&pods_by_status);
    //
    //     let budget_remaining = kube_constraints.api_timeout - Instant::now().duration_since(start);
    //
    //     let (is_satisfied, note) = if nr_running == target_nr_task_managers {
    //         (true, "patch replicas confirmed")
    //     } else if nr_running < target_nr_task_managers && plan.direction() == ScaleDirection::Up {
    //         (false, "expecting taskmanagers to be added")
    //     } else if target_nr_task_managers < nr_running && plan.direction() == ScaleDirection::Down {
    //         (false, "expecting taskmanagers to be removed")
    //     } else {
    //         (
    //             false,
    //             "unexpected number of taskmanagers. Consider adjusting `action.taskmanager.label_selector` \
    //             setting to more specifically match taskmanager pods associated with the targets flink cluster. \
    //             Will continue monitoring in case it settles to expected levels.",
    //         )
    //     };
    //
    //     tracing::info!(
    //         ?correlation,
    //         ?pod_status_counts,
    //         scale_direction=%plan.direction(),
    //         %note,
    //         "patch_replicas: kube found {} taskmanagers in cluster: {} running of {} requested - budget remaining: \
    //          {:?}",
    //         task_managers.len(),
    //         nr_running,
    //         target_nr_task_managers,
    //         budget_remaining
    //     );
    //
    //     Ok((pods_by_status, is_satisfied))
    // }

    #[tracing::instrument(
    level = "info",
    name = "settle_rescaled_replicas::block_for_rescaled_taskmanagers"
        skip(self, plan, flink)
    )]
    async fn block_for_rescaled_taskmanagers(
        &self, plan: &<Self as ScaleAction>::Plan, flink: &FlinkContext,
    ) -> usize {
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
                    nr_confirmed_taskmanagers = tm_detail.nr_taskmanagers;
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
                "settle_rescaled_replicas: flink confirmed {nr_confirmed_taskmanagers} taskmanagers registered \
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

    // fn do_count_running_pods(
    //     pods_by_status: &HashMap<String, Vec<Pod>>,
    // ) -> (usize, HashMap<String, usize>) {
    //     let pod_status_counts: HashMap<String, usize> = pods_by_status
    //         .iter()
    //         .map(|(status, pods)| (status.clone(), pods.len()))
    //         .collect();
    //
    //     let nr_running = pods_by_status
    //         .get(kubernetes::RUNNING_STATUS)
    //         .map(|pods| pods.len())
    //         .unwrap_or(0);
    //
    //     (nr_running, pod_status_counts)
    // }
    //
    // fn group_pods_by_status(pods: &[Pod]) -> HashMap<String, Vec<Pod>> {
    //     let mut result = HashMap::new();
    //
    //     for p in pods {
    //         let status = p
    //             .status
    //             .as_ref()
    //             .and_then(|ps| ps.phase.clone())
    //             .unwrap_or_else(|| kubernetes::UNKNOWN_STATUS.to_string());
    //
    //         let pods_entry = result.entry(status).or_insert_with(Vec::new);
    //         pods_entry.push(p.clone());
    //     }
    //
    //     result
    // }
    //
    // #[tracing::instrument(level = "warn", skip(self, plan, session))]
    // async fn handle_error_on_patch_scale<'s>(
    //     &self, error: KubernetesError, plan: &'s P, session: &'s mut ActionSession,
    // ) -> Result<Option<i32>, KubernetesError> {
    //     let track = format!("{}::patch_scale", self.label());
    //     tracing::error!(
    //         ?error, ?plan, ?session, correlation=%session.correlation(), %track,
    //         "error on patch replicas scale"
    //     );
    //     act::track_act_errors(&track, Some(&error), ActErrorDisposition::Failed, plan);
    //     Err(error)
    // }
    //
    // #[tracing::instrument(level = "warn", skip(self, plan, session))]
    // async fn handle_error_on_patch_block<'s>(
    //     &self, error: ActError, plan: &'s P, session: &'s mut ActionSession,
    // ) -> Result<(), ActError> {
    //     let track = format!("{}::patch_block", self.label());
    //     match error {
    //         ActError::Timeout(duration, ref message) => {
    //             tracing::error!(
    //                 ?plan, ?session, patch_replicas_budget=?duration, correlation=%session.correlation(), %track,
    //                 "{message} - moving on to next action."
    //             );
    //             act::track_act_errors(&track, Some(&error), ActErrorDisposition::Ignored, plan);
    //             Ok(())
    //         },
    //         other => {
    //             tracing::error!(
    //                 error=?other, ?plan, ?session, correlation=%session.correlation(), %track,
    //                 "patch_block error"
    //             );
    //             act::track_act_errors(&track, Some(&other), ActErrorDisposition::Failed, plan);
    //             Err(other)
    //         },
    //     }
    // }
}
