use std::collections::{HashMap, HashSet};
use std::time::Duration;

use async_trait::async_trait;
use either::{Either, Left, Right};
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, StreamExt, TryFutureExt};
use http::{Method, StatusCode};
use once_cell::sync::Lazy;
use proctor::error::UrlError;
use prometheus::{IntCounter, Opts};
use tracing_futures::Instrument;
use url::Url;

use crate::flink::{
    self, FlinkContext, FlinkError, JarId, JobId, JobState, RestoreMode, SavepointLocation,
};
use crate::phases::act;
use crate::phases::act::action::{ActionSession, ScaleAction};
use crate::phases::act::{ActError, ScaleActionPlan};
use crate::phases::plan::ScalePlan;
use crate::settings::FlinkActionSettings;

pub const ACTION_LABEL: &str = "restart_jobs";

#[derive(Debug)]
pub struct RestartJobs {
    pub restart_timeout: Duration,
    pub polling_interval: Duration,
    pub allow_non_restored_state: Option<bool>,
    pub program_args: Option<Vec<String>>,
    pub restore_mode: Option<RestoreMode>,
}

impl RestartJobs {
    pub fn from_settings(settings: &FlinkActionSettings) -> Self {
        Self {
            restart_timeout: settings.restart.operation_timeout,
            polling_interval: settings.polling_interval,
            allow_non_restored_state: settings.restart.allow_non_restored_state,
            program_args: settings.restart.program_args.clone(),
            restore_mode: settings.restart.restore_mode,
        }
    }
}

#[async_trait]
impl ScaleAction for RestartJobs {
    type Plan = ScalePlan;
    type Session = ActionSession;

    fn label(&self) -> &str {
        ACTION_LABEL
    }

    fn check_preconditions(&self, session: &Self::Session) -> Result<(), ActError> {
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
        skip(self)
    )]
    async fn execute<'s>(
        &self, plan: &'s Self::Plan, session: &'s mut Self::Session,
    ) -> Result<(), ActError> {
        let parallelism = Self::parallelism_from_plan_session(plan, session);

        let mut outcome = Ok(());
        if let Some(locations) = Self::locations_from(session) {
            if let Some(ref uploaded_jars) = session.uploaded_jars {
                let remaining_sources = self
                    .try_jar_restarts_for_parallelism(
                        parallelism,
                        uploaded_jars,
                        locations.clone(),
                        session,
                    )
                    .await;

                // -- attempt rollback to original parallelism for each source that failed to restart
                if !remaining_sources.is_empty() {
                    tracing::warn!(
                        nr_restart_failures=%remaining_sources.len(), ?remaining_sources,
                        nr_original_replicas=%plan.current_replicas(),
                        "Flink restart failures occurred - attempting to restart at original parallelism"
                    );

                    let mut jars = Vec::with_capacity(remaining_sources.len());
                    let mut savepoints = HashSet::with_capacity(remaining_sources.len());
                    for (j, s, _e) in remaining_sources {
                        jars.push(j);
                        savepoints.insert(s);
                    }

                    let failures = self
                        .try_jar_restarts_for_parallelism(
                            plan.current_replicas(),
                            &jars,
                            savepoints,
                            session,
                        )
                        .await;

                    if !failures.is_empty() {
                        tracing::error!(
                            nr_restart_failures=%failures.len(), ?failures,
                            "Failed to restart all Flink jobs"
                        );
                        let mut errors = Vec::with_capacity(failures.len());
                        let mut jar_savepoints = Vec::with_capacity(failures.len());
                        for (j, s, e) in failures {
                            errors.push(e.into());
                            jar_savepoints.push((j, s));
                        }

                        outcome = Err(ActError::JobRestart { sources: errors, jar_savepoints });
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

        outcome
    }

    async fn on_error<'s>(&self, error: ActError, plan: &'s Self::Plan, session: &'s mut Self::Session) -> Result<(), ActError> {
        todo!()
    }
}

impl RestartJobs {
    /// Returns the parallelism to use for the restart. In the case of a scale up, this will most
    /// likely be the new parallelism. In the case of a scale down, this will most likely be the
    /// current parallelism. Discrepancies between the two cases are due to rescaling the cluster
    /// partially completed within budgeted time.
    fn parallelism_from_plan_session(plan: &ScalePlan, session: &ActionSession) -> usize {
        let mut parallelism = plan.target_replicas();

        if let Some(nr_tm_confirmed) = session.nr_confirmed_rescaled_taskmanagers {
            if parallelism != nr_tm_confirmed {
                let effective_parallelism = usize::min(parallelism, nr_tm_confirmed);

                tracing::warn!(
                    nr_target_parallelism=%parallelism,
                    nr_confirmed_rescaled_taskmanagers=?session.nr_confirmed_rescaled_taskmanagers,
                    effective_parallelism=%effective_parallelism,
                    correlation=?plan.correlation(),
                    "Target parallelism does not match confirmed rescaled taskmanagers - setting parallelism to minimum of the two."
                );

                parallelism = effective_parallelism;
            }
        }

        parallelism
    }

    async fn try_jar_restarts_for_parallelism(
        &self, parallelism: usize, jars: &[JarId], mut locations: HashSet<SavepointLocation>,
        session: &ActionSession,
    ) -> Vec<(JarId, SavepointLocation, ActError)> {
        use crate::flink::JobState as JS;

        let correlation = session.correlation();
        let span =
            tracing::info_span!("try_jar_restarts_for_parallelism", %parallelism, ?correlation);

        let jar_restarts = self
            .try_all_jar_restarts(jars, &mut locations, parallelism, session)
            .instrument(span.clone())
            .await;

        let job_sources: HashMap<JobId, (JarId, SavepointLocation)> = match jar_restarts {
            Ok(pairings) if pairings.is_empty() => {
                tracing::warn!(
                    %parallelism, jar_ids=?jars, savepoint_location=?locations,
                    "no successful restart found for jars, savepoint locations and parallelism - manual intervention may be necessary."
                );

                HashMap::default()
            },
            Ok(pairings) => {
                let job_pairings = pairings
                    .into_iter()
                    .map(|(job, jar, savepoint)| (job, (jar, savepoint)))
                    .collect();
                tracing::debug!(?job_pairings, "successful restart initiation for pairings.");
                job_pairings
            },
            Err(err) => {
                flink::track_flink_errors("restart_jobs::restart", &err);
                tracing::error!(
                    error=?err, ?correlation,
                    "failure while trying to restart jobs -- may need manual intervention"
                );
                HashMap::default()
            },
        };
        let jobs: Vec<JobId> = job_sources.keys().cloned().collect();

        let restarted = self.block_until_all_jobs_restarted(&jobs, session).instrument(span).await;

        let mut failed: Vec<(JarId, SavepointLocation, ActError)> = Vec::new();
        for (job, outcome) in restarted {
            match outcome {
                Err(err) => {
                    flink::track_flink_errors("restart_jobs::confirm", &err);
                    tracing::error!(
                        error=?err, ?correlation,
                        "failure while waiting for all jobs to restart -- may need manual intervention"
                    );

                    if let Some((jar_id, savepoint_location)) = job_sources.get(&job).cloned() {
                        failed.push((jar_id, savepoint_location, err.into()));
                    }
                },
                Ok(state) if state == JS::Failing || state == JS::Failed => {
                    tracing::error!(%job, job_state=%state, "job failed after restart -- may need manual intervention");
                    if let Some((jar_id, savepoint_location)) = job_sources.get(&job).cloned() {
                        failed.push((
                            jar_id,
                            savepoint_location.clone(),
                            ActError::FailedJob(job, savepoint_location),
                        ));
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
        &self, jars: &[JarId], locations: &mut HashSet<SavepointLocation>, parallelism: usize,
        session: &ActionSession,
    ) -> Result<Vec<(JobId, JarId, SavepointLocation)>, FlinkError> {
        let mut pairings = Vec::with_capacity(jars.len());
        let correlation = session.correlation();

        for jar_id in jars {
            match self
                .try_jar_restart(jar_id, locations.clone(), parallelism, session)
                .await?
            {
                Some((job_id, used_location)) => {
                    locations.remove(&used_location);
                    pairings.push((job_id, jar_id.clone(), used_location));
                },
                None => {
                    track_missed_jar_restarts();
                    tracing::warn!(
                        ?locations, %parallelism, ?correlation,
                        "no savepoint locations match to restart jar -- manual intervention may be required for jar({jar_id}) if it corresponds to an active job."
                    );
                },
            };
        }

        Ok(pairings)
    }

    #[tracing::instrument(level = "trace", skip(session))]
    async fn try_jar_restart(
        &self, jar: &JarId, locations: HashSet<SavepointLocation>, parallelism: usize,
        session: &ActionSession,
    ) -> Result<Option<(JobId, SavepointLocation)>, FlinkError> {
        let mut job_savepoint = None;
        let correlation = session.correlation();

        for location in locations {
            match self
                .try_jar_restart_for_location(jar, &location, parallelism, session)
                .await?
            {
                Left(job_id) => {
                    tracing::info!(%job_id, %parallelism, ?correlation, "restarted job from jar({jar}) + savepoint({location}) pair.");
                    job_savepoint = Some((job_id, location));
                    break;
                },
                Right(http_status) => {
                    tracing::info!(
                        %parallelism, ?correlation, ?http_status,
                        "Flink rejected jar({jar}) + savepoint({location}) pair. Trying next savepoint location."
                    );
                },
            }
        }

        Ok(job_savepoint)
    }

    #[tracing::instrument(level = "trace", skip(session))]
    async fn try_jar_restart_for_location(
        &self, jar: &JarId, location: &SavepointLocation, parallelism: usize,
        session: &ActionSession,
    ) -> Result<Either<JobId, StatusCode>, FlinkError> {
        let correlation = session.correlation();
        let step_label = super::action_step(self.label(), "try_restart_jar_for_location");
        let url = Self::restart_jar_url_for(&session.flink, jar)?;
        let span = tracing::info_span!("act::restart_jobs - restart_jar", %url, ?correlation, %jar, %location, %parallelism);

        let body = restart::RestartJarRequestBody {
            savepoint_path: Some(location.clone()),
            parallelism: Some(parallelism),
            allow_non_restored_state: self.allow_non_restored_state,
            program_args_list: self.program_args.clone(),
            //todo: add feature for supported flink version -- restore_mode: self.restore_mode,
            ..restart::RestartJarRequestBody::default()
        };
        tracing::debug!(request_body=?body, ?correlation, ?url, "restarting jar({jar}) with savepoint({location:?})...");

        let result: Result<Either<JobId, StatusCode>, FlinkError> = session
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
                    tracing::info!(
                        ?response,
                        "jar+savepoint pair rejected by Flink - following error is informational only"
                    );
                }
                flink::log_response(&step_label, &url, &response);

                response
                    .text()
                    .map(move |body: reqwest::Result<String>| {
                        body.map(|b| {
                            tracing::debug!(body=%b, "Flink restart jar with savepoint response body.");
                            (b, status)
                        })
                    })
                    .map_err(|err| err.into())
            })
            .instrument(span)
            .await
            .and_then(|(body, status)| Self::do_assess_restart_response(body.as_str(), status));

        flink::track_result(
            "restart_jar",
            result,
            "trying jar+savepoint pair",
            &correlation,
        )
    }

    fn do_assess_restart_response(
        body: &str, status: StatusCode,
    ) -> Result<Either<JobId, StatusCode>, FlinkError> {
        if status.is_success() {
            serde_json::from_str(body)
                .map(|r: restart::RestartJarResponseBody| Left(r.job_id))
                .map_err(|err| err.into())
        } else {
            Ok(Right(status))
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
        &self, jobs: &[JobId], session: &ActionSession,
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
        &self, job_id: &JobId, session: &ActionSession,
    ) -> Result<JobState, FlinkError> {
        use crate::flink::JobState as JS;

        let correlation = session.correlation();

        let task = async {
            loop {
                match session.flink.query_job_details(job_id, &correlation).await {
                    Ok(detail) if detail.state.is_engaged() => {
                        tracing::debug!(?detail, "job restart succeeded: job is {}", detail.state);
                        break detail.state;
                    },
                    Ok(detail) if detail.state == JS::Failed => {
                        // tracing::error!(?detail, "job failed after restart -- may need manual intervention");
                        tracing::debug!(?detail, "job {} - restart is unsuccessful", detail.state);
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
    fn locations_from(session: &ActionSession) -> Option<HashSet<SavepointLocation>> {
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

    fn check_savepoint_jobs(completed_jobs: HashSet<JobId>, session: &ActionSession) -> bool {
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
            parallelism: Some(27),
            ..restart::RestartJarRequestBody::default()
        };

        assert_tokens(
            &body,
            &[
                Token::Struct { name: "RestartJarRequestBody", len: 2 },
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
