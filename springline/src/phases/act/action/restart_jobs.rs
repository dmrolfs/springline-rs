use crate::Env;
use async_trait::async_trait;
use either::{Either, Left, Right};
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, StreamExt, TryFutureExt};
use http::{Method, StatusCode};
use once_cell::sync::Lazy;
use proctor::error::{MetricLabel, UrlError};
use proctor::{AppData, Correlation};
use prometheus::{IntCounter, Opts};
use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::time::Duration;
use tracing_futures::Instrument;
use url::Url;

use crate::flink::{
    self, FlinkContext, FlinkError, JarId, JobId, JobState, Parallelism, RestoreMode,
    SavepointLocation,
};
use crate::phases::act::action::{ActionSession, ScaleAction};
use crate::phases::act::{self, ActError, ActErrorDisposition};
use crate::phases::plan::ScaleActionPlan;
use crate::settings::FlinkActionSettings;

pub const ACTION_LABEL: &str = "restart_jobs";

//todo: RestartJobs has grown way too complex for this structure. Refactor into an action that spawns
// saga actors to manage state transitions as they progress through process, recoevery, etc. and
// aggregate saga outcomes into final action outcome.
#[derive(Debug)]
pub struct RestartJobs<P> {
    pub restart_timeout: Duration,
    pub polling_interval: Duration,
    pub allow_non_restored_state: Option<bool>,
    pub program_args: Option<Vec<String>>,
    pub restore_mode: Option<RestoreMode>,
    marker: PhantomData<P>,
}

impl<P> RestartJobs<P> {
    pub fn from_settings(settings: &FlinkActionSettings) -> Self {
        Self {
            restart_timeout: settings.restart.operation_timeout,
            polling_interval: settings.polling_interval,
            allow_non_restored_state: settings.restart.allow_non_restored_state,
            program_args: settings.restart.program_args.clone(),
            restore_mode: settings.restart.restore_mode,
            marker: PhantomData,
        }
    }
}

#[async_trait]
impl<P> ScaleAction for RestartJobs<P>
where
    P: AppData + ScaleActionPlan + Correlation,
{
    type Plan = P;

    fn label(&self) -> &str {
        ACTION_LABEL
    }

    fn check_preconditions(&self, session: &Env<ActionSession>) -> Result<(), ActError> {
        match &session.savepoints {
            None => Err(ActError::ActionPrecondition {
                action: self.label().to_string(),
                reason: "savepoints not set".to_string(),
            }),
            Some(locations) if locations.completed.is_empty() => {
                Err(ActError::ActionPrecondition {
                    action: self.label().to_string(),
                    reason: format!(
                        "no savepoint locations found to restart after rescale: failed:{:?}.",
                        locations.failed
                    ),
                })
            },
            _ => Ok(()),
        }?;

        match &session.uploaded_jars {
            None => Err(ActError::ActionPrecondition {
                action: self.label().to_string(),
                reason: "uploaded jars not set".to_string(),
            }),
            Some(jars) if jars.is_empty() => Err(ActError::ActionPrecondition {
                action: self.label().to_string(),
                reason: "no uploaded jars found to restart after rescale".to_string(),
            }),
            _ => Ok(()),
        }?;

        Ok(())
    }

    #[tracing::instrument(
        level = "info",
        name = "RestartFlinkWithNewParallelism::execute",
        skip(self, plan,)
    )]
    async fn execute<'s>(
        &mut self, plan: &'s Self::Plan, session: &'s mut Env<ActionSession>,
    ) -> Result<(), ActError> {
        let parallelism = self.parallelism_from_plan_session(plan, session);

        let mut outcome = Ok(());
        if let Some(locations) = Self::locations_from(session) {
            if let Some(ref uploaded_jars) = session.uploaded_jars {
                let remaining_sources = self
                    .try_jar_restarts_for_parallelism(
                        parallelism,
                        uploaded_jars,
                        locations.clone(),
                        plan,
                        session,
                    )
                    .await;

                // -- attempt rollback to original parallelism for each source that failed to restart
                if !remaining_sources.is_empty() {
                    tracing::warn!(
                        nr_restart_failures=%remaining_sources.len(), ?remaining_sources,
                        nr_original_parallelism=%plan.current_job_parallelism(),
                        "Flink restart failures occurred - attempting to restart at original parallelism"
                    );

                    let nr_initial_failures = remaining_sources.len();
                    let mut savepoints = HashSet::with_capacity(nr_initial_failures);
                    let mut initial_failures = HashMap::with_capacity(nr_initial_failures);
                    for (j, s, e) in remaining_sources {
                        initial_failures.insert(j, e);
                        if let Some(s0) = s {
                            savepoints.insert(s0);
                        }
                    }

                    if !savepoints.is_empty() {
                        let jars: Vec<_> = initial_failures.keys().cloned().collect();
                        let restart_parallelism = plan.current_job_parallelism();
                        let repeat_failures = self
                            .try_jar_restarts_for_parallelism(
                                restart_parallelism,
                                &jars,
                                savepoints,
                                plan,
                                session,
                            )
                            .await;

                        if !repeat_failures.is_empty() {
                            outcome = self
                                .handle_remaining_restart_failures(
                                    repeat_failures,
                                    initial_failures,
                                    plan,
                                    session,
                                )
                                .await;
                        }
                    } else {
                        outcome = Err(ActError::NoSavepointForRestart { initial_failures });
                    }
                }
            } else {
                tracing::warn!(
                    "No uploaded jars to start jobs from -- skipping {}. Flink standalone not supported. \
                     Todo: add identification of standalone mode and once detected apply Reactive Flink approach \
                     (with necessary assumption that Reactive mode is configured.",
                    self.label()
                );
            };
        };

        tracing::info!(?outcome, "final restart outcome");
        outcome
    }
}

impl<P> RestartJobs<P>
where
    P: AppData + ScaleActionPlan + Correlation,
{
    /// Returns the parallelism to use for the restart. In the case of a scale up, this will most
    /// likely be the new parallelism. In the case of a scale down, this will most likely be the
    /// current parallelism. Discrepancies between the two cases are due to rescaling the cluster
    /// partially completed within budgeted time.
    fn parallelism_from_plan_session(&self, plan: &P, session: &Env<ActionSession>) -> Parallelism {
        let mut parallelism = plan.target_job_parallelism();

        if let Some(nr_tm_confirmed) = session.nr_confirmed_rescaled_taskmanagers {
            if plan.target_replicas() != nr_tm_confirmed {
                let confirmed_parallelism_capacity = plan
                    .parallelism_for_replicas(nr_tm_confirmed)
                    .unwrap_or_else(|| Parallelism::new(nr_tm_confirmed.into()));
                let effective_parallelism =
                    Parallelism::min(parallelism, confirmed_parallelism_capacity);

                let track = format!("{}::try_jar_restart::confirmed_below_target", self.label());
                tracing::warn!(
                    ?plan,
                    nr_target_parallelism=%parallelism,
                    nr_confirmed_rescaled_taskmanagers=?nr_tm_confirmed,
                    %effective_parallelism,
                    correlation=?plan.correlation(),
                    %track,
                    "Confirmed rescaled taskmanagers does not match target -- setting parallelism to minimum of the two."
                );
                act::track_act_errors(
                    &track,
                    Option::<&ActError>::None,
                    ActErrorDisposition::Recovered,
                    plan,
                );

                parallelism = effective_parallelism;
            }
        }

        parallelism
    }

    async fn try_jar_restarts_for_parallelism<'s>(
        &self, parallelism: Parallelism, jars: &[JarId], mut locations: HashSet<SavepointLocation>,
        plan: &'s P, session: &'s Env<ActionSession>,
    ) -> Vec<(JarId, Option<SavepointLocation>, ActError)> {
        use crate::flink::JobState as JS;

        let correlation = session.correlation();

        let jar_restarts: Vec<(JarId, Either<(JobId, SavepointLocation), FlinkError>)> = self
            .try_all_jar_restarts(jars, &mut locations, parallelism, plan, session)
            .instrument(
                tracing::info_span!("try_jar_restarts_for_parallelism", %parallelism, %correlation),
            )
            .await;

        let mut failed: Vec<(JarId, Option<SavepointLocation>, ActError)> = Vec::new();

        let restarted_jobs = self.do_handle_jar_restarts(jar_restarts, &mut failed);
        if restarted_jobs.is_empty() {
            let track = format!(
                "{}::try_jar_restart::no_restart_jar_location_pairings",
                self.label()
            );
            tracing::warn!(
                %parallelism, jar_ids=?jars, savepoint_location=?locations, %track, ?failed, %correlation,
                "no successful restart found for jars, savepoint locations and parallelism - manual intervention may be necessary."
            );

            act::track_act_errors(
                &track,
                Option::<&ActError>::None,
                ActErrorDisposition::Failed,
                plan,
            );
        };

        let jobs: Vec<JobId> = restarted_jobs.keys().cloned().collect();
        let restarted = self
            .block_until_all_jobs_restarted(&jobs, session)
            .instrument(
                tracing::info_span!("block_until_all_jobs_restarted", ?jobs, %parallelism, %correlation),
            )
            .await;
        for (job, outcome) in restarted {
            match outcome {
                Err(err) => {
                    if let Err(e) = self.handle_error_on_restart_confirm(err, plan, session).await {
                        if let Some((jar_id, savepoint_location)) =
                            restarted_jobs.get(&job).cloned()
                        {
                            failed.push((jar_id, Some(savepoint_location), e.into()));
                        }
                    }
                },
                Ok(state) if state == JS::Failing || state == JS::Failed => {
                    if let Some((jar_id, savepoint_location)) = restarted_jobs.get(&job) {
                        if let Err(e) = self
                            .handle_failed_job_restart(
                                job,
                                savepoint_location,
                                state,
                                plan,
                                session,
                            )
                            .await
                        {
                            failed.push((jar_id.clone(), Some(savepoint_location.clone()), e));
                        }
                    }
                },
                Ok(state) => {
                    tracing::info!(
                        is_active=%state.is_active(), is_engaged=%state.is_engaged(), ?correlation,
                        "restarted Flink job({job}) with state: {state}"
                    );
                },
            }
        }

        failed
    }

    #[tracing::instrument(level = "trace", skip(session))]
    async fn try_all_jar_restarts(
        &self, jars: &[JarId], locations: &mut HashSet<SavepointLocation>,
        parallelism: Parallelism, plan: &P, session: &Env<ActionSession>,
    ) -> Vec<(JarId, Either<(JobId, SavepointLocation), FlinkError>)> {
        let correlation = session.correlation();
        let mut jar_restarts = Vec::with_capacity(jars.len());

        for jar_id in jars {
            tracing::info!(
                ?locations,
                ?parallelism,
                "attempting to restart jar: {jar_id:?}"
            );
            let restart_attempt =
                self.try_jar_restart(jar_id, locations, parallelism, session).await;
            match restart_attempt {
                Ok(Some((job_id, used_location))) => {
                    locations.remove(&used_location);
                    tracing::info!(
                        ?used_location, ?jar_id, updated_locations=?locations, ?jar_restarts,
                        "restarting jar with job_id={job_id:?}"
                    );
                    jar_restarts.push((jar_id.clone(), Left((job_id, used_location))));
                },
                Ok(None) => {
                    track_missed_jar_restarts();
                    tracing::warn!(
                        ?locations, %parallelism, %correlation,
                        "no savepoint locations match to restart jar -- manual intervention may be required for jar({jar_id}) if it corresponds to an active job."
                    );
                },
                Err(error) => {
                    let track = format!("{}::jar_restart", error.label());
                    tracing::error!(?error, ?jar_id, savepoint_locations=?locations, "failure requesting jar restart for savepoint.");
                    act::track_act_errors(&track, Some(&error), ActErrorDisposition::Failed, plan);
                    jar_restarts.push((jar_id.clone(), Right(error)));
                },
            };
        }

        jar_restarts
    }

    #[tracing::instrument(level = "trace", skip(session))]
    async fn try_jar_restart(
        &self, jar: &JarId, locations: &HashSet<SavepointLocation>, parallelism: Parallelism,
        session: &Env<ActionSession>,
    ) -> Result<Option<(JobId, SavepointLocation)>, FlinkError> {
        let correlation = session.correlation();
        let mut job_savepoint = None;
        let mut restart_error = None;
        for location in locations {
            match self
                .try_jar_restart_for_location(jar, location, parallelism, session)
                .await
            {
                Ok(job_id) => {
                    tracing::info!(%job_id, %parallelism, ?correlation, "restarted job from jar({jar}) + savepoint({location}) pair.");
                    job_savepoint = Some((job_id, location.clone()));
                    break;
                },
                // Ok(Right(http_status)) => {
                //     tracing::info!(
                //         %parallelism, ?correlation, ?http_status,
                //         "Flink rejected jar({jar}) + savepoint({location}) pair. Trying next savepoint location."
                //     );
                //     WORK HERBE PLACE ERROR client_error or server_error
                // },
                Err(error) => {
                    tracing::error!(
                        ?error,
                        ?jar,
                        ?location,
                        "restart attempt failed for jar+location"
                    );
                    restart_error = Some(error);
                },
            }
        }

        restart_error.map(Err).unwrap_or(Ok(job_savepoint))
    }

    #[tracing::instrument(level = "debug", skip(session))]
    async fn try_jar_restart_for_location(
        &self, jar: &JarId, location: &SavepointLocation, parallelism: Parallelism,
        session: &Env<ActionSession>,
    ) -> Result<JobId, FlinkError> {
        let correlation = session.correlation();
        let step_label = super::action_step(self.label(), "try_restart_jar_for_location");
        let url = Self::restart_jar_url_for(&session.flink, jar)?;
        let span = tracing::info_span!("act::restart_jobs - restart_jar", %url, ?correlation, %jar, %location, %parallelism);

        let body = restart::RestartJarRequestBody {
            savepoint_path: Some(location.clone()),
            parallelism: Some(
                usize::try_from(parallelism.as_u32())
                    .map_err(|err| FlinkError::Other(err.into()))?,
            ),
            allow_non_restored_state: self.allow_non_restored_state,
            entry_class: session.entry_class.clone(),
            program_args_list: self.program_args.clone(),
            //todo: add feature for supported flink version -- restore_mode: self.restore_mode,
            ..restart::RestartJarRequestBody::default()
        };
        tracing::debug!(request_body=?body, ?correlation, ?url, "restarting jar({jar}) with savepoint({location:?})...");

        let restart_result = session
            .flink
            .client()
            .request(Method::POST, url.clone())
            .json(&body)
            .send()
            .map_err(|error| {
                tracing::error!(%error, "failed to restart jar");
                error.into()
            })
            .and_then(|response| {
                let status = response.status();
                if !status.is_success() {
                    tracing::warn!(
                        ?response,
                        "jar+savepoint pair rejected by Flink - see response detail in following log entry."
                    );
                }
                flink::log_response(&step_label, &url, &response);

                response
                    .text()
                    .map(move |body: reqwest::Result<String>| {
                        body.map(|b| {
                            if status.is_client_error() || status.is_server_error() {
                                tracing::error!(?status, body=%b, "Flink restart jar failed with response body.");
                            } else {
                                tracing::debug!(?status, body=%b, "Flink restart jar with savepoint response body.");
                            }

                            (b, status)
                        })
                    })
                    .map_err(|err| err.into())
            })
            .instrument(span)
            .await
            .and_then(|(body, status)| Self::do_assess_restart_response(jar, location, parallelism, body.as_str(), status));

        // tracing::warn!(?restart_result, "DMR: flink restart job post request");

        flink::track_result(
            "restart_jar",
            restart_result,
            "trying jar+savepoint pair",
            correlation,
        )
    }

    fn do_assess_restart_response(
        jar_id: &JarId, location: &SavepointLocation, parallelism: Parallelism, body: &str,
        status: StatusCode,
    ) -> Result<JobId, FlinkError> {
        match status {
            s if s.is_success() => serde_json::from_str(body)
                .map(|r: restart::RestartJarResponseBody| r.job_id)
                .map_err(|err| err.into()),
            s if s.is_client_error() || s.is_server_error() => Err(FlinkError::FailedJobRestart {
                status,
                jar_id: jar_id.clone(),
                location: location.clone(),
                parallelism,
                cause: body.to_string(),
            }),
            _ => Err(FlinkError::UnsupportedRestartResponse { status, cause: body.to_string() }),
        }
    }

    fn restart_jar_url_for(flink: &FlinkContext, jar_id: &JarId) -> Result<Url, UrlError> {
        let mut endpoint_url = flink.jars_endpoint();
        endpoint_url
            .path_segments_mut()
            .map_err(|_| UrlError::UrlCannotBeBase(flink.jars_endpoint()))?
            .push(jar_id.as_ref())
            .push("run");
        Ok(endpoint_url)
    }

    #[tracing::instrument(
        level = "trace",
        name = "restart_jobs::block_until_all_jobs_restarted",
        skip(session),
        fields(correlation = %session.correlation()),
    )]
    async fn block_until_all_jobs_restarted(
        &self, jobs: &[JobId], session: &Env<ActionSession>,
    ) -> Vec<(JobId, Result<JobState, FlinkError>)> {
        let mut tasks = jobs
            .iter()
            .map(|job_id| async {
                let state = self.wait_on_job_restart(job_id, session).await;
                (job_id.clone(), state)
            })
            .collect::<FuturesUnordered<_>>();

        let mut result = Vec::with_capacity(jobs.len());
        while let Some((job_id, job_state)) = tasks.next().await {
            tracing::info!(%job_id, ?job_state, "job restart outcome.");
            result.push((job_id, job_state));
        }

        result
    }

    #[tracing::instrument(level = "trace", skip(session), fields(correlation = %session.correlation()))]
    async fn wait_on_job_restart(
        &self, job_id: &JobId, session: &Env<ActionSession>,
    ) -> Result<JobState, FlinkError> {
        use crate::flink::JobState as JS;

        let correlation_id = session.correlation().relabel();

        let task = async {
            loop {
                match session.flink.query_job_details(job_id, &correlation_id).await {
                    Ok(detail) if detail.state.is_engaged() => {
                        tracing::debug!(?detail, "job restart succeeded: job is {}", detail.state);
                        break detail.state;
                    },
                    Ok(detail) if detail.state == JS::Failed => {
                        // tracing::error!(?detail, "job failed after restart -- may need manual intervention");
                        tracing::warn!(?detail, "job {} - restart is unsuccessful", detail.state);
                        break detail.state;
                    },
                    Ok(detail) if detail.state.is_stopped() => {
                        tracing::warn!(
                            ?detail,
                            "job stopped({}) after restart -- may need manual intervention",
                            detail.state
                        );
                        break detail.state;
                    },
                    Ok(detail) => {
                        tracing::debug!(
                            ?detail,
                            "job detail received:{}, but job is not engaged - checking again in {:?}",
                            detail.state,
                            self.polling_interval
                        )
                    },
                    Err(err) => {
                        // todo: consider capping attempts
                        tracing::warn!(
                            %err,
                            "failed to query job details - checking again in {:?}",
                            self.polling_interval
                        );
                    },
                }

                tokio::time::sleep(self.polling_interval).await;
            }
        };

        tokio::time::timeout(self.restart_timeout, task).await.map_err(|_elapsed| {
            FlinkError::Timeout(
                format!("Timed out waiting for job({job_id}) to restart"),
                self.restart_timeout,
            )
        })
    }

    #[allow(clippy::cognitive_complexity)]
    fn locations_from(session: &Env<ActionSession>) -> Option<HashSet<SavepointLocation>> {
        let nr_savepoints = session.savepoints.as_ref().map(|s| s.completed.len()).unwrap_or(0);
        let mut jobs = HashSet::with_capacity(nr_savepoints);
        let mut locations = HashSet::with_capacity(nr_savepoints);
        for (job, location) in session.savepoints.iter().flat_map(|s| s.completed.iter()) {
            jobs.insert(job.clone());
            locations.insert(location.clone());
        }

        if Self::check_savepoint_jobs(jobs, session) {
            Some(locations)
        } else {
            None
        }
    }

    fn check_savepoint_jobs(completed_jobs: HashSet<JobId>, session: &Env<ActionSession>) -> bool {
        let correlation = session.correlation();

        if completed_jobs.is_empty() {
            tracing::warn!(
                ?session,
                ?correlation,
                "No savepoints found in session to restart - skipping {}.",
                ACTION_LABEL
            );
            false
        } else {
            let active_jobs = session
                .active_jobs
                .as_ref()
                .map(|jobs| jobs.iter().cloned().collect())
                .unwrap_or_else(HashSet::new);
            let completed_inactive_jobs: HashSet<&JobId> =
                completed_jobs.difference(&active_jobs).collect();
            if !completed_inactive_jobs.is_empty() {
                tracing::warn!(
                    ?completed_inactive_jobs,
                    ?correlation,
                    "Found completed savepoints for jobs that were not active. This is unexpected and may indicate a \
                     bug in the application."
                );
            }
            true
        }
    }

    #[tracing::instrument(level = "debug", skip(self, pairings, failed_restarts))]
    fn do_handle_jar_restarts<'s>(
        &self, pairings: Vec<(JarId, Either<(JobId, SavepointLocation), FlinkError>)>,
        failed_restarts: &mut Vec<(JarId, Option<SavepointLocation>, ActError)>,
    ) -> HashMap<JobId, (JarId, SavepointLocation)> {
        let restarted_jobs = pairings
            .into_iter()
            .filter_map(
                |(jar, job_savepoint_or_error)| match job_savepoint_or_error {
                    Left((job, savepoint)) => Some((job, (jar, savepoint))),
                    Right(error) => {
                        failed_restarts.push((jar, None, error.into()));
                        tracing::debug!(?failed_restarts, "FAILED job and savepoint",);
                        None
                    },
                },
            )
            .collect();

        tracing::debug!(?restarted_jobs, ?failed_restarts, "job restart outcomes.");
        restarted_jobs
    }

    #[tracing::instrument(level = "debug", skip(self, plan, session))]
    async fn handle_error_on_restart_confirm<'s>(
        &self, error: FlinkError, plan: &'s P, session: &'s Env<ActionSession>,
    ) -> Result<(), FlinkError> {
        flink::track_flink_errors("restart_jobs::confirm", &error);
        let track = format!("{}::restart_confirm", self.label());
        tracing::error!(
            ?error, ?plan, ?session, correlation=?session.correlation(), %track,
            "failure while waiting for all jobs to restart -- may need manual intervention"
        );
        act::track_act_errors(&track, Some(&error), ActErrorDisposition::Failed, plan);
        Err(error)
    }

    #[tracing::instrument(level = "debug", skip(self, job, location, job_state, plan, session))]
    async fn handle_failed_job_restart<'s>(
        &self, job: JobId, location: &SavepointLocation, job_state: JobState, plan: &'s P,
        session: &'s Env<ActionSession>,
    ) -> Result<(), ActError> {
        let track = format!("{}::failed_job_restart", self.label());
        tracing::error!(
            %job, %job_state, ?plan, ?session, correlation=?session.correlation(), %track,
            "job failed after restart -- may need manual intervention"
        );

        act::track_act_errors(
            &track,
            Option::<&ActError>::None,
            ActErrorDisposition::Failed,
            plan,
        );
        Err(ActError::FailedJob(job, location.clone()))
    }

    #[tracing::instrument(level = "debug", skip(self, plan, session))]
    async fn handle_remaining_restart_failures<'s>(
        &self, repeat_failures: Vec<(JarId, Option<SavepointLocation>, ActError)>,
        initial_failures: HashMap<JarId, ActError>, plan: &'s P, session: &'s Env<ActionSession>,
    ) -> Result<(), ActError> {
        use ActError as ActE;

        let repeat_failures: Vec<_> = repeat_failures
            .into_iter()
            .map(|(j, s, e2)| {
                let e1 = initial_failures.get(&j);
                (j, s, e1, e2)
            })
            .collect();

        tracing::error!(
            nr_restart_failures=%repeat_failures.len(), ?repeat_failures,
            ?plan, ?session, correlation=?session.correlation(),
            "Job restart failures remain after multiple attempts."
        );

        let mut errors = Vec::with_capacity(repeat_failures.len());
        let mut jar_savepoints = Vec::with_capacity(repeat_failures.len());
        let mut possible_depleted_taskmanagers = false;

        let track = format!("{}::remaining_restart_failure", self.label());
        for (j, s, e1, e2) in repeat_failures {
            if let (Some(ActE::FailedJob(_, _)), ActE::FailedJob(job_id, location)) = (e1, &e2) {
                tracing::warn!(
                    ?job_id, jar_id=?j, ?location, error=?e2, initial_error=?e1,
                    "repeated FailedJob restart suggests possible depleted taskmanager(s)."
                );
                possible_depleted_taskmanagers = true;
            }

            tracing::error!(
                error=?e2, initial_error=?e1, label=%self.label(), jar_id=?j, savepoint_location=?s, %track,
                "remaining_restart_failure"
            );
            act::track_act_errors(&track, Some(&e2), ActErrorDisposition::Failed, plan);
            errors.push(e2.into());
            if let Some(s0) = s {
                jar_savepoints.push((j, s0));
            }
        }

        Err(ActE::JobRestart {
            sources: errors,
            jar_savepoints,
            possible_depleted_taskmanagers,
        })
    }
}

mod restart {
    use serde::{Deserialize, Serialize};
    use serde_with::serde_as;

    use crate::flink::{JobId, RestoreMode, SavepointLocation};

    #[serde_as]
    #[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(default, rename_all = "camelCase")]
    pub struct RestartJarRequestBody {
        /// Boolean value that specifies whether the job submission should be rejected if the
        /// savepoint contains state that cannot be mapped back to the job.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub allow_non_restored_state: Option<bool>,

        /// String value that specifies the fully qualified name of the entry point class.
        /// Overrides the class defined in the jar file manifest.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub entry_class: Option<String>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub job_id: Option<JobId>,

        /// Positive integer value that specifies the desired parallelism for the job.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub parallelism: Option<usize>,

        /// Comma-separated list of program arguments.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub program_args: Option<String>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub program_args_list: Option<Vec<String>>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[serde_as(as = "Option<serde_with::DisplayFromStr>")]
        pub restore_mode: Option<RestoreMode>,

        /// String value that specifies the path of the savepoint to restore the job from.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub savepoint_path: Option<SavepointLocation>,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "kebab-case")]
    pub struct RestartJarResponseBody {
        /// The ID of the job that was restarted.
        #[serde(rename = "jobid")]
        pub job_id: JobId,
    }
}

#[inline]
pub fn track_missed_jar_restarts() {
    FLINK_MISSED_JAR_RESTARTS.inc()
}

pub static FLINK_MISSED_JAR_RESTARTS: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::with_opts(
        Opts::new(
            "flink_missed_jar_restarts",
            "Number of jars failed to find a savepoint to restart.",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating flink_missed_jar_restarts metric")
});

#[cfg(test)]
mod tests {
    use claim::*;
    use pretty_assertions::assert_eq;
    use serde_test::{assert_tokens, Token};

    use super::*;

    #[test]
    fn test_common_restart_jar_restart_body_ser_json() {
        let body = restart::RestartJarRequestBody {
            savepoint_path: Some(SavepointLocation::new("/path/to/savepoint")),
            parallelism: Some(27),
            ..restart::RestartJarRequestBody::default()
        };

        let actual = assert_ok!(serde_json::to_string(&body));
        assert_eq!(
            actual,
            r##"{"parallelism":27,"savepointPath":"/path/to/savepoint"}"##
        )
    }

    #[test]
    fn test_common_restart_jar_restart_body_serde_tokens() {
        let body = restart::RestartJarRequestBody {
            savepoint_path: Some(SavepointLocation::new("/path/to/savepoint")),
            entry_class: Some("com.example.foo.Bar".to_string()),
            parallelism: Some(27),
            ..restart::RestartJarRequestBody::default()
        };

        assert_tokens(
            &body,
            &[
                Token::Struct { name: "RestartJarRequestBody", len: 3 },
                Token::Str("entryClass"),
                Token::Some,
                Token::Str("com.example.foo.Bar"),
                Token::Str("parallelism"),
                Token::Some,
                Token::U64(27),
                Token::Str("savepointPath"),
                Token::Some,
                Token::NewtypeStruct { name: "SavepointLocation" },
                Token::Str("/path/to/savepoint"),
                Token::StructEnd,
            ],
        );
    }

    #[test]
    fn test_restart_jar_request_body_serde_tokens() {
        let body = restart::RestartJarRequestBody {
            allow_non_restored_state: Some(true),
            entry_class: Some("org.apache.flink.examples.WordCount".to_string()),
            job_id: Some(JobId::new("job-id-1")),
            parallelism: Some(27),
            program_args: Some("-Dinput=/path/to/input,-Doutput=/path/to/output".to_string()),
            program_args_list: Some(vec!["foo".to_string(), "bar".to_string()]),
            restore_mode: Some(RestoreMode::Claim),
            savepoint_path: Some(SavepointLocation::new("/path/to/savepoint")),
        };

        assert_tokens(
            &body,
            &vec![
                Token::Struct { name: "RestartJarRequestBody", len: 8 },
                Token::Str("allowNonRestoredState"),
                Token::Some,
                Token::Bool(true),
                Token::Str("entryClass"),
                Token::Some,
                Token::Str("org.apache.flink.examples.WordCount"),
                Token::Str("jobId"),
                Token::Some,
                Token::NewtypeStruct { name: "JobId" },
                Token::Str("job-id-1"),
                Token::Str("parallelism"),
                Token::Some,
                Token::U64(27),
                Token::Str("programArgs"),
                Token::Some,
                Token::Str("-Dinput=/path/to/input,-Doutput=/path/to/output"),
                Token::Str("programArgsList"),
                Token::Some,
                Token::Seq { len: Some(2) },
                Token::Str("foo"),
                Token::Str("bar"),
                Token::SeqEnd,
                Token::Str("restoreMode"),
                Token::Some,
                Token::Str("CLAIM"),
                Token::Str("savepointPath"),
                Token::Some,
                Token::NewtypeStruct { name: "SavepointLocation" },
                Token::Str("/path/to/savepoint"),
                Token::StructEnd,
            ],
        )
    }
}
