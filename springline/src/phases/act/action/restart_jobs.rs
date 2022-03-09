use crate::flink;
use crate::flink::{FlinkContext, FlinkError, JarId, JobId, JobSavepointReport, JobState, SavepointLocation};
use crate::model::CorrelationId;
use crate::phases::act::action::{ActionSession, ScaleAction};
use crate::phases::act::scale_actuator::ScaleActionPlan;
use crate::phases::act::ActError;
use crate::settings::FlinkActionSettings;
use async_trait::async_trait;
use either::{Either, Left, Right};
use futures_util::stream::{FuturesOrdered, FuturesUnordered};
use futures_util::{FutureExt, StreamExt, TryFutureExt};
use http::{Method, StatusCode};
use once_cell::sync::Lazy;
use proctor::error::UrlError;
use proctor::AppData;
use prometheus::{HistogramOpts, HistogramTimer, HistogramVec, IntCounter};
use std::collections::HashSet;
use std::time::Duration;
use tracing_futures::Instrument;
use url::Url;

pub const ACTION_LABEL: &str = "restart_jobs";

#[derive(Debug, Clone)]
pub struct RestartJobs<P> {
    pub flink: FlinkContext,
    pub restart_timeout: Duration,
    pub polling_interval: Duration,
    marker: std::marker::PhantomData<P>,
}

impl<P> RestartJobs<P> {
    pub const fn from_settings(flink: FlinkContext, settings: &FlinkActionSettings) -> Self {
        let polling_interval = settings.polling_interval;
        let restart_timeout = settings.restart_operation_timeout;
        Self {
            flink,
            restart_timeout,
            polling_interval,
            marker: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<P> ScaleAction<P> for RestartJobs<P>
where
    P: AppData + ScaleActionPlan,
{
    #[tracing::instrument(level = "info", name = "RestartFlinkWithNewParallelism::execute", skip(self))]
    async fn execute(
        &mut self, mut session: ActionSession<P>,
    ) -> Result<ActionSession<P>, (ActError, ActionSession<P>)> {
        let timer = start_flink_restart_job_timer(&self.flink);
        let correlation = session.correlation();

        let parallelism = session.plan.target_replicas();

        if let Some(locations) = Self::locations_from(&session, &correlation) {
            if let Some(ref uploaded_jars) = session.uploaded_jars {
                let jobs = match self
                    .try_all_jar_restarts(uploaded_jars, locations, parallelism, &correlation)
                    .await
                {
                    Ok(jobs) => jobs,
                    Err(err) => {
                        flink::track_flink_errors("restart_jobs::restart", &err);
                        tracing::error!(
                            error=?err, %correlation,
                            "failure while trying to restart jobs -- need manual intervention"
                        );
                        Vec::default()
                    },
                };

                if let Err(err) = self.block_until_all_jobs_restarted(&jobs, &correlation).await {
                    flink::track_flink_errors("restart_jobs::confirm", &err);
                    tracing::error!(
                        error=?err, %correlation,
                        "failure while waiting for all jobs to restart -- need manual intervention"
                    );
                }
            } else {
                tracing::warn!(
                    ?session, %correlation,
                    "No uploaded jars to start jobs from -- skipping {ACTION_LABEL}. Flink standalone not supported. Todo: add identification of standalone mode and once detected apply Reactive Flink approach (with necessary assumption that Reactive mode is configured."
                );
            };
        }

        Ok(session.with_duration(ACTION_LABEL, Duration::from_secs_f64(timer.stop_and_record())))
    }
}

impl<P> RestartJobs<P>
where
    P: AppData + ScaleActionPlan,
{
    #[tracing::instrument(level = "info", skip(self))]
    async fn try_all_jar_restarts(
        &self, jars: &[JarId], mut locations: HashSet<SavepointLocation>, parallelism: usize,
        correlation: &CorrelationId,
    ) -> Result<Vec<JobId>, FlinkError> {
        let mut jobs = Vec::with_capacity(jars.len());

        for jar_id in jars {
            let mut used_location = None;

            match self
                .try_restart_locations_for_jar(jar_id, locations.clone(), parallelism, correlation)
                .await?
            {
                Some((job_id, location)) => {
                    jobs.push(job_id);
                    used_location = Some(location);
                },
                None => {
                    track_missed_jar_restarts();
                    tracing::error!(
                        %jar_id, ?locations, %parallelism, %correlation,
                        "no savepoint locations match to restart jar -- manual intervention may be required"
                    );
                },
            };

            if let Some(location_to_remove) = used_location {
                locations.remove(&location_to_remove);
            }
        }

        Ok(jobs)
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn try_restart_locations_for_jar(
        &self, jar_id: &JarId, locations: HashSet<SavepointLocation>, parallelism: usize, correlation: &CorrelationId,
    ) -> Result<Option<(JobId, SavepointLocation)>, FlinkError> {
        let mut job_savepoint = None;

        for location in locations {
            match self.try_restart(jar_id, &location, parallelism, correlation).await? {
                Left(job_id) => {
                    tracing::info!(%job_id, %parallelism, %correlation, "restarted job from jar({jar_id}) + savepoint({location}) pair.");
                    job_savepoint = Some((job_id, location));
                    break;
                },
                Right(http_status) => {
                    tracing::info!(
                        %parallelism, %correlation, ?http_status,
                        "Flink rejected jar({jar_id}) + savepoint({location}) pair. Trying next savepoint location."
                    );
                },
            }
        }

        Ok(job_savepoint)
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn try_restart(
        &self, jar: &JarId, location: &SavepointLocation, parallelism: usize, correlation: &CorrelationId,
    ) -> Result<Either<JobId, StatusCode>, FlinkError> {
        let url = self.restart_jar_url_for(jar)?;
        let span = tracing::info_span!("restart_jar", %url, %correlation, %jar, %location, %parallelism);

        let body = restart::RestartJarRequestBody {
            savepoint_path: Some(location.clone()),
            parallelism: Some(parallelism),
            ..restart::RestartJarRequestBody::default()
        };

        let result: Result<Either<JobId, StatusCode>, FlinkError> = self
            .flink
            .client()
            .request(Method::POST, url)
            .json(&body)
            .send()
            .map_err(|error| {
                tracing::error!(%error, "failed to restart jar");
                error.into()
            })
            .and_then(|response| {
                flink::log_response(ACTION_LABEL, &response);
                let status = response.status();

                response
                    .text()
                    .map(move |body: reqwest::Result<String>| body.map(|b| (b, status)))
                    .map_err(|err| err.into())
            })
            .instrument(span)
            .await
            .and_then(|(body, status)| Self::do_assess_restart_response(body.as_str(), status));

        flink::track_result("restart_jar", result, "trying jar+savepoint pair", correlation)
    }

    fn do_assess_restart_response(body: &str, status: StatusCode) -> Result<Either<JobId, StatusCode>, FlinkError> {
        if status.is_success() {
            serde_json::from_str(body)
                .map(|r: restart::RestartJarResponseBody| {
                    tracing::info!(%body, restart_response=?r, "Restart jar response");
                    Left(r.job_id)
                })
                .map_err(|err| err.into())
        } else {
            Ok(Right(status))
        }
    }

    fn restart_jar_url_for(&self, jar_id: &JarId) -> Result<Url, UrlError> {
        let mut endpoint_url = self.flink.jars_endpoint();
        endpoint_url
            .path_segments_mut()
            .map_err(|_| UrlError::UrlCannotBeBase(self.flink.jars_endpoint()))?
            .push(jar_id.as_ref())
            .push("run");
        Ok(endpoint_url)
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn block_until_all_jobs_restarted(
        &self, jobs: &[JobId], correlation: &CorrelationId,
    ) -> Result<(), FlinkError> {
        let mut tasks = jobs
            .iter()
            .map(|job_id| async {
                self.wait_on_job_restart(job_id, correlation)
                    .await
                    .map(|job_state| (job_id.clone(), job_state))
            })
            .collect::<FuturesOrdered<_>>();

        while let Some(task) = tasks.next().await {
            let (job_id, job_state) = task?;
            tracing::info!("job restarted: job({job_id}): {job_state}");
        }

        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn wait_on_job_restart(&self, job: &JobId, correlation: &CorrelationId) -> Result<JobState, FlinkError> {
        let task = async {
            let mut result = None;
            while result.is_none() {
                match self.flink.query_job_details(job, correlation).await {
                    Ok(detail) => {
                        tracing::info!(%job, %correlation, ?detail, "job detail received");
                        result = Some(detail.state);
                    },
                    Err(err) => {
                        //todo: consider capping attempts
                        tracing::error!(%job, %correlation, %err, "failed to query job details");
                    },
                }

                tokio::time::sleep(self.polling_interval).await;
            }

            result.expect("job restart should have been populated")
        };

        tokio::time::timeout(self.restart_timeout, task).await.map_err(|_elapsed| {
            FlinkError::Timeout(
                format!("Timed out waiting for job({job}) to restart"),
                self.restart_timeout,
            )
        })
    }

    #[allow(clippy::cognitive_complexity)]
    fn locations_from(session: &ActionSession<P>, correlation: &CorrelationId) -> Option<HashSet<SavepointLocation>> {
        let nr_savepoints = session.savepoints.as_ref().map(|s| s.completed.len()).unwrap_or(0);
        let mut jobs = HashSet::with_capacity(nr_savepoints);
        let mut locations = HashSet::with_capacity(nr_savepoints);
        for (job, location) in session.savepoints.iter().flat_map(|s| s.completed.iter()) {
            jobs.insert(job.clone());
            locations.insert(location.clone());
        }

        if Self::check_savepoint_jobs(jobs, session, correlation) {
            Some(locations)
        } else {
            None
        }
    }

    fn check_savepoint_jobs(
        completed_jobs: HashSet<JobId>, session: &ActionSession<P>, correlation: &CorrelationId,
    ) -> bool {
        if completed_jobs.is_empty() {
            tracing::warn!(?session, %correlation, "No savepoints found in session to restart - skipping {ACTION_LABEL}.");
            false
        } else {
            let active_jobs = session
                .active_jobs
                .as_ref()
                .map(|jobs| jobs.iter().cloned().collect())
                .unwrap_or_else(|| HashSet::new());
            let completed_inactive_jobs: HashSet<&JobId> = completed_jobs.difference(&active_jobs).collect();
            if !completed_inactive_jobs.is_empty() {
                tracing::warn!(
                    ?completed_inactive_jobs, %correlation,
                    "Found completed savepoints for jobs that were not active. This is unexpected and may indicate a bug in the application."
                );
            }
            true
        }
    }
}

mod restart {
    use crate::flink::{FlinkError, JobId, SavepointLocation};
    use serde::{Deserialize, Serialize};
    use serde_with::serde_as;
    use std::fmt;

    #[serde_as]
    #[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(default, rename_all = "camelCase")]
    pub struct RestartJarRequestBody {
        /// Boolean value that specifies whether the job submission should be rejected if the
        /// savepoint contains state that cannot be mapped back to the job.
        #[serde(skip_serializing_if = "Option::is_none")]
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
        #[serde(default)]
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

    #[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub enum RestoreMode {
        /// Flink will take ownership of the given snapshot. It will clean the snapshot once it is
        /// subsumed by newer ones.
        Claim,

        /// Flink will not claim ownership of the snapshot files. However it will make sure it does
        /// not depend on any artefacts from the restored snapshot. In order to do that, Flink will
        /// take the first checkpoint as a full one, which means it might reupload/duplicate files
        /// that are part of the restored checkpoint.
        NoClaim,

        /// This is the mode in which Flink worked so far. It will not claim ownership of the
        /// snapshot and will not delete the files. However, it can directly depend on the existence
        /// of the files of the restored checkpoint. It might not be safe to delete checkpoints that
        /// were restored in legacy mode
        Legacy,
    }

    impl fmt::Display for RestoreMode {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Claim => write!(f, "CLAIM"),
                Self::NoClaim => write!(f, "NO-CLAIM"),
                Self::Legacy => write!(f, "LEGACY"),
            }
        }
    }

    impl std::str::FromStr for RestoreMode {
        type Err = FlinkError;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            match s.to_uppercase().as_str() {
                "CLAIM" => Ok(Self::Claim),
                "NO-CLAIM" => Ok(Self::NoClaim),
                "LEGACY" => Ok(Self::Legacy),
                _ => Err(FlinkError::UnexpectedValue {
                    expected: "CLAIM, NO-CLAIM, LEGACY".to_string(),
                    given: s.to_string(),
                }),
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "kebab-case")]
    pub struct RestartJarResponseBody {
        /// The ID of the job that was restarted.
        pub job_id: JobId,
    }
}

#[inline]
pub(crate) fn track_missed_jar_restarts() {
    FLINK_MISSED_JAR_RESTARTS.inc()
}

pub static FLINK_MISSED_JAR_RESTARTS: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new(
        "flink_missed_jar_restarts",
        "Number of jars failed to find a savepoint to restart.",
    )
    .expect("failed creating flink_missed_jar_restarts metric")
});

#[inline]
fn start_flink_restart_job_timer(context: &FlinkContext) -> HistogramTimer {
    FLINK_RESTART_JOB_TIME.with_label_values(&[context.label()]).start_timer()
}

pub static FLINK_RESTART_JOB_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "flink_job_restart_job_time_seconds",
            "Time spent restarting a job to complete",
        )
        .buckets(vec![1., 1.5, 2., 3., 4., 5., 10.0, 25., 50., 75., 100.]),
        &["label"],
    )
    .expect("failed creating flink_restart_job_time_seconds histogram metric")
});

#[cfg(test)]
mod tests {
    use super::*;
    use claim::*;
    use pretty_assertions::assert_eq;
    use serde_json::json;
    use serde_test::{assert_tokens, Token};

    #[test]
    fn test_common_restart_jar_restart_body_ser_json() {
        let body = restart::RestartJarRequestBody {
            savepoint_path: Some(SavepointLocation::new("/path/to/savepoint")),
            parallelism: Some(27),
            ..restart::RestartJarRequestBody::default()
        };

        let actual = assert_ok!(serde_json::to_string());
        assert_eq!(actual, r##"{"savepointPath":"/path/to/savepoint","parallelism":27}"##)
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
                Token::Struct { name: "RestartJarRequestBody", len: 3 },
                Token::Str("savepointPath"),
                Token::Some,
                Token::Str("/path/to/savepoint"),
                Token::Str("parallelism"),
                Token::Some,
                Token::U64(27),
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
            restore_mode: Some(restart::RestoreMode::Claim),
            savepoint_path: Some(SavepointLocation::new("/path/to/savepoint")),
        };

        assert_tokens(
            &body,
            &vec![
                Token::Struct { name: "RestartJarRequestBody", len: 9 },
                Token::Str("allowNonRestoredState"),
                Token::Some,
                Token::Bool(true),
                Token::Str("entryClass"),
                Token::Some,
                Token::Str("org.apache.flink.examples.WordCount"),
                Token::Str("jobId"),
                Token::Some,
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
                Token::Str("/path/to/savepoint"),
                Token::StructEnd,
            ],
        )
    }
}
