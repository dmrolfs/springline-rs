use std::collections::HashSet;
use std::future::Future;
use std::marker::PhantomData;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::{FutureExt, TryFutureExt};
use http::Method;
use proctor::error::UrlError;
use proctor::AppData;
use tokio::time::Instant;
use tracing::Instrument;
use url::Url;

use super::{ActionSession, ScaleAction};
use crate::flink::{
    self, FlinkContext, FlinkError, JobId, JobSavepointReport, OperationStatus, SavepointStatus,
};
use crate::phases::act::{self, ActError, ActErrorDisposition, ScaleActionPlan};
use crate::settings::FlinkActionSettings;
use crate::CorrelationId;

pub const ACTION_LABEL: &str = "savepoint";

#[derive(Debug)]
pub struct CancelWithSavepoint<P> {
    pub polling_interval: Duration,
    pub savepoint_timeout: Duration,
    pub savepoint_dir: Option<String>,
    marker: PhantomData<P>,
}

impl<P> CancelWithSavepoint<P> {
    pub fn from_settings(settings: &FlinkActionSettings) -> Self {
        Self {
            polling_interval: settings.polling_interval,
            savepoint_timeout: settings.savepoint.operation_timeout,
            savepoint_dir: settings.savepoint.directory.clone(),
            marker: PhantomData,
        }
    }
}

#[async_trait]
impl<P> ScaleAction for CancelWithSavepoint<P>
where
    P: AppData + ScaleActionPlan,
{
    type Plan = P;

    fn label(&self) -> &str {
        ACTION_LABEL
    }

    fn check_preconditions(&self, session: &ActionSession) -> Result<(), ActError> {
        match &session.active_jobs {
            None => Err(ActError::ActionPrecondition {
                action: self.label().to_string(),
                reason: "active jobs not set".to_string(),
            }),
            Some(jobs) if jobs.is_empty() => Err(ActError::ActionPrecondition {
                action: self.label().to_string(),
                reason: "no active jobs found to rescale".to_string(),
            }),
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
        name = "StopFlinkWithSavepoint::execute",
        skip(self, plan, session)
    )]
    async fn execute<'s>(
        &self, plan: &'s Self::Plan, session: &'s mut ActionSession,
    ) -> Result<(), ActError> {
        let active_jobs = session.active_jobs.clone().unwrap_or_default();

        let job_triggers = match Self::trigger_savepoints_for(&active_jobs, session).await {
            Err(err) => self.handle_error_on_trigger(err, plan, session).await,
            triggers => triggers,
        }?;

        let savepoint_report = match self.do_collect_savepoint_report(&job_triggers, session).await
        {
            Err(err) => {
                self.handle_error_on_collect_savepoint(err, &job_triggers, plan, session)
                    .await
            },
            report => report,
        }?;

        let cancelled = match self.do_wait_for_job_cancel(&job_triggers, session).await {
            Err(err) => {
                self.handle_error_on_wait_for_cancellations(err, &job_triggers, plan, session)
                    .await
            },
            c => c,
        }?;

        let triggers: HashSet<_> = job_triggers.into_iter().map(|jt| jt.0).collect();
        let remaining: HashSet<_> = triggers.difference(&cancelled).collect();
        if !remaining.is_empty() {
            let action_label = format!("{}::unconfirmed_cancellation", self.label());
            (0..remaining.len()).into_iter().for_each(|_| {
                act::track_act_errors(
                    &action_label,
                    Option::<&ActError>::None,
                    ActErrorDisposition::Ignored,
                    plan,
                )
            });
            tracing::warn!(?remaining, "jobs not confirmed to be cancelled")
        }

        session.savepoints = Some(savepoint_report);
        Ok(())
    }
}

impl<P> CancelWithSavepoint<P>
where
    P: AppData + ScaleActionPlan,
{
    #[tracing::instrument(level = "info", skip(self, job_triggers, session))]
    async fn do_collect_savepoint_report<'s>(
        &self, job_triggers: &[(JobId, trigger::TriggerId)], session: &'s ActionSession,
    ) -> Result<JobSavepointReport, ActError> {
        let mut tasks = Vec::new();
        for (job, trigger) in job_triggers.iter().cloned() {
            let job_info = Self::wait_on_savepoint(
                job.clone(),
                trigger,
                self.polling_interval,
                self.savepoint_timeout,
                session,
            )
            .map(|info| (job, info));

            tasks.push(job_info);
        }

        Self::block_for_all_savepoints(tasks, &session.correlation()).await
    }

    #[tracing::instrument(level = "info", skip(self, job_triggers, session))]
    async fn do_wait_for_job_cancel<'s>(
        &self, job_triggers: &[(JobId, trigger::TriggerId)], session: &'s ActionSession,
    ) -> Result<HashSet<JobId>, ActError> {
        let mut tasks = Vec::new();
        for (job, _) in job_triggers.iter().cloned() {
            let cancelled = Self::wait_on_job_cancel(
                job.clone(),
                self.polling_interval,
                self.savepoint_timeout,
                session,
            )
            .map(|outcome| (job, outcome));
            tasks.push(cancelled);
        }

        Self::block_for_all_cancellations(tasks, &session.correlation())
            .instrument(tracing::debug_span!("block for all job cancellations"))
            .await
            .map(|job_ids| job_ids.into_iter().collect())

        // Future<Output = (JobId, Result<SavepointStatus, FlinkError>)> + Send,
    }

    async fn trigger_savepoints_for(
        jobs: &[JobId], session: &ActionSession,
    ) -> Result<Vec<(JobId, trigger::TriggerId)>, ActError> {
        let mut job_triggers = Vec::with_capacity(jobs.len());
        let mut trigger_failures = Vec::new();
        for job in jobs.iter().cloned() {
            let trigger_result = Self::trigger_savepoint(&job, true, session).await;
            match trigger_result {
                Ok(trigger_id) => job_triggers.push((job, trigger_id)),
                Err(err) => trigger_failures.push((job, err)),
            }
        }

        if !trigger_failures.is_empty() {
            let (first_job, source) = trigger_failures.remove(0);
            let mut job_ids = Vec::with_capacity(trigger_failures.len());
            job_ids.push(first_job);
            job_ids.extend(trigger_failures.into_iter().map(|(job, _)| job));
            return Err(ActError::Savepoint { source, job_ids });
        }

        Ok(job_triggers)
    }

    async fn trigger_savepoint(
        job_id: &JobId, cancel_job: bool, session: &ActionSession,
    ) -> Result<trigger::TriggerId, FlinkError> {
        let step_label = super::action_step(ACTION_LABEL, "trigger_savepoint");
        let url = Self::trigger_endpoint_url_for(&session.flink, job_id)?;
        let span = tracing::info_span!(
            "act::savepoint - trigger_savepoint",
            ?job_id, %cancel_job, %url, correlation=%session.correlation()
        );

        let body = trigger::SavepointRequestBody {
            cancel_job,
            ..trigger::SavepointRequestBody::default()
        };

        let trigger_id: Result<trigger::TriggerId, FlinkError> = session
            .flink
            .client()
            .request(Method::POST, url.clone())
            .json(&body)
            .send()
            .map_err(|error| {
                tracing::warn!(%error, "Failed to trigger savepoint in Flink for job {job_id}");
                error.into()
            })
            .and_then(|response| {
                flink::log_response(&step_label, &url, &response);
                response.text().map(|body| {
                    body.map_err(|err| err.into()).and_then(|b| {
                        let trigger_response: Result<trigger::SavepointTriggerResponseBody, FlinkError> =
                            serde_json::from_str(&b).map_err(|err| err.into());
                        tracing::info!(body=%b, ?trigger_response, "Savepoint triggered response body.");
                        trigger_response.map(|r| r.request_id)
                    })
                })
            })
            .instrument(span)
            .await;

        match trigger_id {
            Ok(tid) => Ok(tid),
            Err(err) => {
                tracing::warn!(error=?err, "failed to trigger savepoint in Flink for job {job_id}");
                flink::track_flink_errors(ACTION_LABEL, &err);
                Err(err)
            },
        }
    }

    fn trigger_endpoint_url_for(flink: &FlinkContext, id: &JobId) -> Result<Url, UrlError> {
        let mut endpoint_url = flink.jobs_endpoint();
        endpoint_url
            .path_segments_mut()
            .map_err(|_| UrlError::UrlCannotBeBase(flink.jobs_endpoint()))?
            .push(id.as_ref())
            .push("savepoints");
        Ok(endpoint_url)
    }

    #[tracing::instrument(level = "debug", skip(session))]
    async fn wait_on_job_cancel(
        job: JobId, polling_interval: Duration, cancel_timeout: Duration, session: &ActionSession,
    ) -> Result<(), FlinkError> {
        let task = async {
            let start = Instant::now();

            loop {
                let job_detail =
                    session.flink.query_job_details(&job, &session.correlation()).await;
                match job_detail {
                    Ok(details) if details.state.is_stopped() => {
                        tracing::info!(job_status=%details.state, "job {job} is not active.");
                        break;
                    },
                    Ok(details) => {
                        let elapsed = start.elapsed();
                        tracing::info!(
                            job_status=%details.state,
                            "job {job} is active - checking again in {polling_interval:?} - budget remaining: {remaining:?}.",
                            remaining = cancel_timeout - elapsed,
                        );
                    },
                    Err(err) => {
                        tracing::warn!(
                            error=?err, ?job, correlation=%session.correlation(),
                            "check on cancellation of job {job} failed - checking again in {polling_interval:?}.",
                        );
                    },
                }

                tokio::time::sleep(polling_interval).await;
            }
        };

        tokio::time::timeout(cancel_timeout, task).await.map_err(|_elapsed| {
            FlinkError::Timeout(format!("flink cancel job {job}"), cancel_timeout)
        })
    }

    #[tracing::instrument(level = "trace", skip(session))]
    async fn wait_on_savepoint(
        job: JobId, trigger: trigger::TriggerId, polling_interval: Duration,
        savepoint_timeout: Duration, session: &ActionSession,
    ) -> Result<SavepointStatus, FlinkError> {
        let task = async {
            let start = Instant::now();

            loop {
                match Self::check_savepoint(&job, &trigger, session).await {
                    Ok(savepoint) if savepoint.status == OperationStatus::Completed => {
                        tracing::info!(?savepoint, "savepoint completed.");
                        break savepoint;
                    },
                    Ok(savepoint) => {
                        let elapsed = start.elapsed();
                        tracing::info!(
                            ?savepoint,
                            "savepoint in progress - checking again in {polling_interval:?} - budget remaining: {remaining:?}.",
                            remaining = savepoint_timeout - elapsed,
                        )
                    },
                    Err(err) => {
                        // todo: consider capping attempts
                        tracing::warn!(
                            error=?err, ?trigger, correlation=%session.correlation(),
                            "check on savepoint operation for {job} failed - checking again in {polling_interval:?}."
                        );
                    },
                }

                tokio::time::sleep(polling_interval).await;
            }
        };

        tokio::time::timeout(savepoint_timeout, task).await.map_err(|_elapsed| {
            FlinkError::Timeout(
                format!("flink savepoint (job:{job}, trigger:{trigger})"),
                savepoint_timeout,
            )
        })
    }

    #[tracing::instrument(level = "trace", skip(job_id, trigger_id, session))]
    async fn check_savepoint(
        job_id: &JobId, trigger_id: &trigger::TriggerId, session: &ActionSession,
    ) -> Result<SavepointStatus, FlinkError> {
        let step_label = super::action_step(ACTION_LABEL, "check_savepoint");
        let url = Self::savepoint_info_url_for(&session.flink, job_id, trigger_id)?;
        let span = tracing::info_span!(
            "act::savepoint - check_savepoint", %job_id, %trigger_id, %url, correlation=%session.correlation()
        );

        let info: Result<SavepointStatus, FlinkError> = session
            .flink
            .client()
            .request(Method::GET, url.clone())
            .send()
            .map_err(|error| {
                tracing::warn!(%error, "Failed to get Flink savepoint info for job {job_id}");
                error.into()
            })
            .and_then(|response| {
                flink::log_response(&step_label, &url, &response);
                response.text().map(|body| {
                    body.map_err(|err| err.into()).and_then(|b| {
                        tracing::debug!(body=%b, "savepoint body");
                        let info_response: Result<query::SavepointInfoResponseBody, FlinkError> =
                            serde_json::from_str(&b).map_err(|err| err.into());
                        tracing::debug!(?info_response, "Savepoint info received");
                        info_response.and_then(|resp| resp.try_into())
                    })
                })
            })
            .instrument(span)
            .await;

        match info {
            Ok(info) => Ok(info),
            Err(err) => {
                // tracing::warn!(error=?err, "failed to get Flink savepoint info for job {job_id}");
                flink::track_flink_errors(ACTION_LABEL, &err);
                Err(err)
            },
        }
    }

    fn savepoint_info_url_for(
        flink: &FlinkContext, job_id: &JobId, trigger_id: &trigger::TriggerId,
    ) -> Result<Url, UrlError> {
        let mut endpoint_url = flink.jobs_endpoint();
        endpoint_url
            .path_segments_mut()
            .map_err(|_| UrlError::UrlCannotBeBase(flink.jobs_endpoint()))?
            .push(job_id.as_ref())
            .push("savepoints")
            .push(trigger_id.as_ref());
        Ok(endpoint_url)
    }

    #[tracing::instrument(level = "trace", skip(tasks))]
    async fn block_for_all_savepoints<F>(
        tasks: Vec<F>, correlation: &CorrelationId,
    ) -> Result<JobSavepointReport, ActError>
    where
        F: Future<Output = (JobId, Result<SavepointStatus, FlinkError>)> + Send,
    {
        let mut errors = Vec::new();
        let mut completed = Vec::with_capacity(tasks.len());
        let mut failed = Vec::new();
        for (job, savepoint) in futures::future::join_all(tasks).await {
            match savepoint {
                Err(err) => errors.push((job, err)),
                Ok(s) if s.status != OperationStatus::Completed => {
                    tracing::error!(%job, savepoint=?s, "savepoint task finished but remains IN_PROGRESS - this should not happen");
                    let error = FlinkError::UnexpectedSavepointStatus(job.clone(), s);
                    errors.push((job, error));
                },
                Ok(s) if s.operation.is_none() => {
                    tracing::error!(%job, savepoint=?s, "savepoint task finished but no operation was returned - this should not happen");
                    failed.push((job, s))
                },
                Ok(s) if s.operation.as_ref().unwrap().is_right() => {
                    let cause = s.operation.as_ref().and_then(|o| o.as_ref().right()).unwrap();
                    tracing::info!(%job, savepoint=?s, "savepoint task completed but failed: {cause}");
                    failed.push((job, s));
                },
                Ok(s) => {
                    tracing::info!(%job, savepoint=?s, "savepoint task completed successfully");
                    completed.push((job, s))
                },
            }
        }

        if !errors.is_empty() {
            let failed_jobs = errors.iter().map(|(job, _)| job).cloned().collect();
            tracing::warn!(
                nr_savepoint_errors=%errors.len(), ?correlation, error=?errors.first().unwrap(),
                "failed to complete savepoints - cannot scale Flink server"
            );
            let source = errors.remove(0).1;
            return Err(ActError::Savepoint { source, job_ids: failed_jobs });
        }

        JobSavepointReport::new(completed, failed).map_err(|err| err.into())
    }

    #[tracing::instrument(level = "trace", skip(tasks))]
    async fn block_for_all_cancellations<F>(
        tasks: Vec<F>, correlation: &CorrelationId,
    ) -> Result<Vec<JobId>, ActError>
    where
        F: Future<Output = (JobId, Result<(), FlinkError>)> + Send,
    {
        let mut errors = Vec::new();
        let mut completed = Vec::with_capacity(tasks.len());
        // let mut failed = Vec::new();
        for (job, cancel_outcome) in futures::future::join_all(tasks).await {
            match cancel_outcome {
                Err(err) => errors.push((job, err)),
                Ok(_) => {
                    tracing::info!(%job, "cancel job completed successfully");
                    completed.push(job)
                },
            }
        }

        if !errors.is_empty() {
            let failed_jobs = errors.iter().map(|(job, _)| job).cloned().collect();
            tracing::warn!(
                nr_job_cancel_errors=%errors.len(), ?correlation, error=?errors.first().unwrap(),
                "failed to cancel jobs - cannot scale Flink server"
            );
            let source = errors.remove(0).1;
            return Err(ActError::Savepoint { source, job_ids: failed_jobs });
        }

        Ok(completed)
    }

    #[tracing::instrument(level = "warn", skip(self, job_triggers, plan, session))]
    async fn handle_error_on_wait_for_cancellations<'s>(
        &self, error: ActError, job_triggers: &[(JobId, trigger::TriggerId)], plan: &'s P,
        session: &'s ActionSession,
    ) -> Result<HashSet<JobId>, ActError> {
        tracing::error!(
            ?error,
            ?job_triggers,
            ?plan,
            ?session,
            "error on wait for job cancellations"
        );
        act::track_act_errors(
            &format!("{}::wait_for_cancellations", self.label()),
            Some(&error),
            ActErrorDisposition::Failed,
            plan,
        );
        Err(error)
    }

    #[tracing::instrument(level = "warn", skip(self, plan, session))]
    async fn handle_error_on_trigger<'s>(
        &self, error: ActError, plan: &'s P, session: &'s mut ActionSession,
    ) -> Result<Vec<(JobId, trigger::TriggerId)>, ActError> {
        tracing::error!(?error, ?plan, ?session, "error on trigger savepoint+cancel");
        act::track_act_errors(
            &format!("{}::trigger", self.label()),
            Some(&error),
            ActErrorDisposition::Failed,
            plan,
        );
        Err(error)
    }

    #[tracing::instrument(level = "warn", skip(self, job_triggers, plan, session))]
    async fn handle_error_on_collect_savepoint<'s>(
        &self, error: ActError, job_triggers: &[(JobId, trigger::TriggerId)], plan: &'s P,
        session: &'s ActionSession,
    ) -> Result<JobSavepointReport, ActError> {
        tracing::error!(
            ?error,
            ?job_triggers,
            ?plan,
            ?session,
            "error on collect savepoint report"
        );
        act::track_act_errors(
            &format!("{}::collect_savepoint", self.label()),
            Some(&error),
            ActErrorDisposition::Failed,
            plan,
        );
        Err(error)
    }
}

mod trigger {
    use std::fmt;

    use serde::{Deserialize, Serialize};
    use serde_with::serde_as;

    use crate::flink::FlinkError;

    pub type TriggerId = String;

    #[serde_as]
    #[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(default, rename_all = "kebab-case")]
    pub struct SavepointRequestBody {
        pub cancel_job: bool,

        #[serde(rename = "formatType", skip_serializing_if = "Option::is_none")]
        #[serde_as(as = "Option<serde_with::DisplayFromStr>")]
        pub format_type: Option<SavepointFormatType>,

        #[serde(skip_serializing_if = "Option::is_none")]
        pub target_directory: Option<String>,

        #[serde(rename = "triggerId", skip_serializing_if = "Option::is_none")]
        pub trigger_id: Option<String>,
    }

    #[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize)]
    pub enum SavepointFormatType {
        Canonical,
        Native,
    }

    impl fmt::Display for SavepointFormatType {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "{}",
                match self {
                    Self::Canonical => "CANONICAL",
                    Self::Native => "NATIVE",
                }
            )
        }
    }

    impl std::str::FromStr for SavepointFormatType {
        type Err = FlinkError;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            match s.to_uppercase().as_str() {
                "CANONICAL" => Ok(Self::Canonical),
                "NATIVE" => Ok(Self::Native),
                rep => Err(FlinkError::UnexpectedValue {
                    expected: "CANONICAL or NATIVE".to_string(),
                    given: rep.to_string(),
                }),
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "kebab-case")]
    pub struct SavepointTriggerResponseBody {
        pub request_id: String,
    }
}

mod query {
    use std::fmt;

    use either::Either;
    use serde::{Deserialize, Serialize};
    use serde_with::serde_as;

    use crate::flink::{
        FailureCause, FlinkError, OperationStatus, SavepointLocation, SavepointStatus,
    };

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct SavepointInfoResponseBody {
        pub status: QueueStatus,

        #[serde(skip_serializing_if = "Option::is_none")]
        pub operation: Option<SavepointOperation>,
    }

    impl TryFrom<SavepointInfoResponseBody> for SavepointStatus {
        type Error = FlinkError;

        fn try_from(body: SavepointInfoResponseBody) -> Result<Self, Self::Error> {
            let operation = body.operation.map(TryFrom::try_from).transpose()?;
            Ok(Self { status: body.status.into(), operation })
        }
    }

    #[serde_as]
    #[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct QueueStatus {
        #[serde_as(as = "serde_with::DisplayFromStr")]
        pub id: QueueState,
    }

    impl From<QueueStatus> for OperationStatus {
        fn from(that: QueueStatus) -> Self {
            match that.id {
                QueueState::InProgress => Self::InProgress,
                QueueState::Completed => Self::Completed,
            }
        }
    }

    #[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "kebab-case")]
    pub struct SavepointOperation {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub location: Option<String>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub failure_cause: Option<FailureCause>,
    }

    impl TryFrom<SavepointOperation> for Either<SavepointLocation, FailureCause> {
        type Error = FlinkError;

        fn try_from(that: SavepointOperation) -> Result<Self, Self::Error> {
            if let Some(cause) = that.failure_cause {
                Ok(Self::Right(cause))
            } else if let Some(location) = that.location {
                Ok(Self::Left(SavepointLocation::from(location)))
            } else {
                Err(FlinkError::UnexpectedValue {
                    expected: "location or failure_cause".to_string(),
                    given: format!("{:?}", that),
                })
            }
        }
    }

    #[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub enum QueueState {
        InProgress,
        Completed,
    }

    impl fmt::Display for QueueState {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::InProgress => write!(f, "IN_PROGRESS"),
                Self::Completed => write!(f, "COMPLETED"),
            }
        }
    }

    impl std::str::FromStr for QueueState {
        type Err = FlinkError;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            match s.to_uppercase().as_str() {
                "IN_PROGRESS" => Ok(Self::InProgress),
                "COMPLETED" => Ok(Self::Completed),
                rep => Err(FlinkError::UnexpectedValue {
                    expected: "IN_PROGRESS or COMPLETED".to_string(),
                    given: rep.to_string(),
                }),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use claim::*;
    use pretty_assertions::assert_eq;
    use serde_json::json;
    use serde_test::{assert_tokens, Token};

    use super::*;
    use crate::phases::act::action::savepoint::query::{
        QueueState, QueueStatus, SavepointOperation,
    };

    #[test]
    fn test_savepoint_body_serde_tokens() {
        let body = trigger::SavepointRequestBody {
            cancel_job: true,
            format_type: Some(trigger::SavepointFormatType::Canonical),
            target_directory: Some("/tmp/savepoint".to_string()),
            trigger_id: None,
        };

        assert_tokens(
            &body,
            &vec![
                Token::Struct { name: "SavepointRequestBody", len: 3 },
                Token::Str("cancel-job"),
                Token::Bool(true),
                Token::Str("formatType"),
                Token::Some,
                Token::Str("CANONICAL"),
                Token::Str("target-directory"),
                Token::Some,
                Token::Str("/tmp/savepoint"),
                Token::StructEnd,
            ],
        )
    }

    #[test]
    fn test_savepoint_body_null_operation_json() {
        let body = r##"{"status":{"id":"IN_PROGRESS"},"operation":null}"##;
        let actual: query::SavepointInfoResponseBody = assert_ok!(serde_json::from_str(body));
        assert_eq!(
            actual,
            query::SavepointInfoResponseBody {
                status: QueueStatus { id: QueueState::InProgress },
                operation: None,
            }
        );
    }

    #[test]
    fn test_savepoint_info_serde_json() {
        let success = json!({
            "status": { "id": "COMPLETED" },
            "operation": { "location": "s3a://dev-flink-58dz/foo/savepoints/savepoint-957152-e82cbb4804b1"}
        });
        let success_rep = assert_ok!(serde_json::to_string(&success));

        let actual: query::SavepointInfoResponseBody =
            assert_ok!(serde_json::from_str(&success_rep));
        assert_eq!(
            actual,
            query::SavepointInfoResponseBody {
                status: QueueStatus { id: QueueState::Completed },
                operation: Some(SavepointOperation {
                    location: Some(
                        "s3a://dev-flink-58dz/foo/savepoints/savepoint-957152-e82cbb4804b1"
                            .to_string()
                    ),
                    failure_cause: None,
                }),
            }
        )
    }
}
