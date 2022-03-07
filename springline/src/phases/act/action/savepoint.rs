use crate::flink;
use crate::flink::{FlinkContext, FlinkError, JobId, JobSavepointReport, OperationStatus, SavepointStatus};
use crate::phases::act::action::{ActionSession, ScaleAction};
use crate::phases::act::scale_actuator::ScaleActionPlan;
use crate::phases::act::ActError;

use crate::settings::FlinkActionSettings;
use async_trait::async_trait;
use futures_util::{FutureExt, TryFutureExt};
use http::Method;
use once_cell::sync::Lazy;

use crate::model::CorrelationId;
use proctor::error::UrlError;
use proctor::AppData;
use prometheus::{HistogramOpts, HistogramTimer, HistogramVec};
use std::future::Future;
use std::time::Duration;
use tracing::Instrument;
use url::Url;

pub const ACTION_LABEL: &str = "trigger_savepoint";

#[derive(Debug, Clone)]
pub struct TriggerSavepoint<P> {
    pub flink: FlinkContext,
    pub polling_interval: Duration,
    pub savepoint_timeout: Duration,
    pub savepoint_dir: Option<String>,
    marker: std::marker::PhantomData<P>,
}

impl<P> TriggerSavepoint<P> {
    pub fn from_settings(flink: FlinkContext, settings: &FlinkActionSettings) -> Self {
        let polling_interval = settings.polling_interval;
        let savepoint_timeout = settings.savepoint.operation_timeout;
        let savepoint_dir = settings.savepoint.directory.clone();

        Self {
            flink,
            polling_interval,
            savepoint_timeout,
            savepoint_dir,
            marker: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<P> ScaleAction<P> for TriggerSavepoint<P>
where
    P: AppData + ScaleActionPlan,
{
    #[tracing::instrument(level = "info", name = "StopFlinkWithSavepoint::execute", skip(self))]
    async fn execute(
        &mut self, mut session: ActionSession<P>,
    ) -> Result<ActionSession<P>, (ActError, ActionSession<P>)> {
        let timer = start_flink_job_savepoint_with_cancel_timer(&self.flink);

        let correlation = session.correlation();
        let active_jobs: Vec<JobId> = self
            .flink
            .query_active_jobs(&correlation)
            .await
            .map_err(|err| (err.into(), session.clone()))
            .map(|jobs| jobs.into_iter().map(|j| j.id).collect())?;

        let job_triggers: Vec<(JobId, trigger::TriggerId)> = self
            .trigger_savepoints_for(&active_jobs, &correlation)
            .await
            .map_err(|err| (err, session.clone()))?;

        session.active_jobs = Some(active_jobs);

        let tasks = job_triggers
            .into_iter()
            .map(|(job, trigger)| {
                self.wait_on_savepoint(job.clone(), trigger, &correlation)
                    .map(|savepoint_info| (job, savepoint_info))
            })
            .collect::<Vec<_>>();

        let savepoint_report = Self::block_for_all_savepoints(tasks, &correlation)
            .await
            .map_err(|err| (err, session.clone()))?;

        session.savepoints = Some(savepoint_report);
        Ok(session.with_duration(ACTION_LABEL, Duration::from_secs_f64(timer.stop_and_record())))
    }
}

impl<P> TriggerSavepoint<P>
where
    P: AppData + ScaleActionPlan,
{
    async fn trigger_savepoints_for(
        &self, jobs: &[JobId], correlation: &CorrelationId,
    ) -> Result<Vec<(JobId, trigger::TriggerId)>, ActError> {
        let mut job_triggers = Vec::with_capacity(jobs.len());
        let mut trigger_failures = Vec::new();
        for job in jobs.iter().cloned() {
            let trigger_result = self.trigger_savepoint(&job, true, correlation).await;
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

    #[tracing::instrument(level = "info", skip(self))]
    async fn trigger_savepoint(
        &self, job_id: &JobId, cancel_job: bool, correlation: &CorrelationId,
    ) -> Result<trigger::TriggerId, FlinkError> {
        let url = self.trigger_endpoint_url_for(job_id)?;
        let span = tracing::info_span!("trigger_savepoint", %url, %correlation);

        let body = trigger::SavepointRequestBody {
            cancel_job,
            ..trigger::SavepointRequestBody::default()
        };

        let trigger_id: Result<trigger::TriggerId, FlinkError> = self
            .flink
            .client()
            .request(Method::POST, url)
            .json(&body)
            .send()
            .map_err(|error| {
                tracing::error!(%error, "Failed to trigger savepoint in Flink for job {job_id}");
                error.into()
            })
            .and_then(|response| {
                flink::log_response(ACTION_LABEL, &response);
                response.text().map_err(|err| err.into())
            })
            .instrument(span)
            .await
            .and_then(|body| {
                let trigger_response: Result<trigger::SavepointTriggerResponseBody, FlinkError> =
                    serde_json::from_str(body.as_str()).map_err(|err| err.into());
                tracing::info!(%body, ?trigger_response, "Savepoint triggered");
                trigger_response.map(|r| r.request_id)
            });

        match trigger_id {
            Ok(tid) => Ok(tid),
            Err(err) => {
                tracing::error!(error=?err, "failed to trigger savepoint in Flink for job {job_id}");
                flink::track_flink_errors(ACTION_LABEL, &err);
                Err(err)
            },
        }
    }

    fn trigger_endpoint_url_for(&self, id: &JobId) -> Result<Url, UrlError> {
        let mut endpoint_url = self.flink.jobs_endpoint();
        endpoint_url
            .path_segments_mut()
            .map_err(|_| UrlError::UrlCannotBeBase(self.flink.jobs_endpoint()))?
            .push(id.as_ref())
            .push("savepoints");
        Ok(endpoint_url)
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn wait_on_savepoint(
        &self, job: JobId, trigger: trigger::TriggerId, correlation: &CorrelationId,
    ) -> Result<SavepointStatus, FlinkError> {
        let task = async {
            let mut result = None;
            while result.is_none() {
                match self.check_savepoint(&job, &trigger, correlation).await {
                    Ok(savepoint) if savepoint.status == OperationStatus::Completed => {
                        result = Some(savepoint);
                        break;
                    },
                    Ok(savepoint) => tracing::info!(?savepoint, "savepoint in progress - checking again"),
                    Err(err) => {
                        //todo: consider capping attempts
                        tracing::warn!(
                            error=?err, ?trigger, %correlation,
                            "check on savepoint operation for {job} failed - checking again."
                        );
                    },
                }

                tokio::time::sleep(self.savepoint_timeout).await;
            }

            result.expect("savepoint status should have been populated")
        };

        tokio::time::timeout(self.savepoint_timeout, task)
            .await
            .map_err(|_elapsed| {
                FlinkError::Timeout(
                    format!("flink savepoint (job:{job}, trigger:{trigger})"),
                    self.savepoint_timeout,
                )
            })
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn check_savepoint(
        &self, job_id: &JobId, trigger_id: &trigger::TriggerId, correlation: &CorrelationId,
    ) -> Result<SavepointStatus, FlinkError> {
        let url = self.info_endpoint_url_for(job_id, trigger_id)?;
        let span = tracing::info_span!("check_savepoint", %job_id, %trigger_id, %url, %correlation);

        let info: Result<SavepointStatus, FlinkError> = self
            .flink
            .client()
            .request(Method::GET, url)
            .send()
            .map_err(|error| {
                tracing::error!(%error, "Failed to get Flink savepoint info for job {job_id}");
                error.into()
            })
            .and_then(|response| {
                flink::log_response(ACTION_LABEL, &response);
                response.text().map_err(|err| err.into())
            })
            .instrument(span)
            .await
            .and_then(|body: String| {
                let info_response: Result<query::SavepointInfoResponseBody, FlinkError> =
                    serde_json::from_str(body.as_str()).map_err(|err| err.into());
                tracing::info!(%body, ?info_response, "Savepoint info received");
                info_response.map(|resp| resp.into())
            });

        match info {
            Ok(info) => Ok(info),
            Err(err) => {
                tracing::error!(error=?err, "failed to get Flink savepoint info for job {job_id}");
                flink::track_flink_errors(ACTION_LABEL, &err);
                Err(err)
            },
        }
    }

    fn info_endpoint_url_for(&self, job_id: &JobId, trigger_id: &trigger::TriggerId) -> Result<Url, UrlError> {
        let mut endpoint_url = self.flink.jobs_endpoint();
        endpoint_url
            .path_segments_mut()
            .map_err(|_| UrlError::UrlCannotBeBase(self.flink.jobs_endpoint()))?
            .push(job_id.as_ref())
            .push("savepoints")
            .push(trigger_id.as_ref());
        Ok(endpoint_url)
    }

    #[tracing::instrument(level = "info", skip(tasks))]
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
                Ok(s) if s.operation.is_right() => {
                    tracing::error!(%job, savepoint=?s, "savepoint task completed but failed: {}", s.operation.as_ref().right().unwrap());
                    // let error = FlinkError::UnexpectedSavepointStatus(job.clone(), s);
                    failed.push((job, s));
                },
                Ok(s) => completed.push((job, s)),
            }
        }

        if !errors.is_empty() {
            let failed_jobs = errors.iter().map(|(job, _)| job).cloned().collect();
            tracing::error!(
                nr_savepoint_errors=%errors.len(), %correlation, error=?errors.first().unwrap(),
                "failed to complete savepoints - cannot scale Flink server"
            );
            let source = errors.remove(0).1;
            return Err(ActError::Savepoint { source, job_ids: failed_jobs });
        }

        let report = JobSavepointReport::new(completed, failed);
        Ok(report)
    }
}

#[inline]
fn start_flink_job_savepoint_with_cancel_timer(context: &FlinkContext) -> HistogramTimer {
    FLINK_JOB_SAVEPOINT_WITH_CANCEL_TIME
        .with_label_values(&[context.label()])
        .start_timer()
}

pub static FLINK_JOB_SAVEPOINT_WITH_CANCEL_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "flink_job_savepoint_with_cancel_time_seconds",
            "Time spent waiting for savepoint with cancel to complete",
        )
        .buckets(vec![0.01, 0.015, 0.02, 0.03, 0.04, 0.05, 0.1, 0.25, 0.5, 0.75, 1.0]),
        &["label"],
    )
    .expect("failed creating flink_job_savepoint_with_cancel_time_seconds histogram metric")
});

mod trigger {
    use crate::flink::FlinkError;
    use serde::{Deserialize, Serialize};
    use serde_with::serde_as;
    use std::fmt;

    pub type TriggerId = String;

    #[serde_as]
    #[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
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

    #[derive(Debug, Copy, Clone, PartialEq, Serialize)]
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
                    SavepointFormatType::Canonical => "CANONICAL",
                    SavepointFormatType::Native => "NATIVE",
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

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "kebab-case")]
    pub struct SavepointTriggerResponseBody {
        pub request_id: String,
    }
}

mod query {
    use crate::flink::{FailureReason, FlinkError, OperationStatus, SavepointLocation, SavepointStatus};
    use crate::phases::act::ActError;
    use either::Either;
    use serde::{Deserialize, Serialize};
    use serde_with::serde_as;
    use std::fmt;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct SavepointInfoResponseBody {
        pub status: QueueStatus,
        pub operation: SavepointOperation,
    }

    impl From<SavepointInfoResponseBody> for SavepointStatus {
        fn from(info: SavepointInfoResponseBody) -> Self {
            Self {
                status: info.status.into(),
                operation: TryFrom::try_from(info.operation).expect("invalid savepoint operation"),
            }
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
    pub struct SavepointOperation {
        pub location: Option<String>,
        pub failure_reason: Option<String>,
    }

    impl TryFrom<SavepointOperation> for Either<SavepointLocation, FailureReason> {
        type Error = ActError;

        fn try_from(that: SavepointOperation) -> Result<Self, Self::Error> {
            if let Some(reason) = that.failure_reason {
                Ok(Self::Right(FailureReason::from(reason)))
            } else if let Some(location) = that.location {
                Ok(Self::Left(SavepointLocation::from(location)))
            } else {
                Err(ActError::Flink(FlinkError::UnexpectedValue {
                    expected: "location or failure_reason".to_string(),
                    given: format!("{:?}", that),
                }))
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
    use super::*;
    use crate::phases::act::action::savepoint::query::{QueueState, QueueStatus, SavepointOperation};
    use claim::*;
    use pretty_assertions::assert_eq;
    use serde_json::json;
    use serde_test::{assert_tokens, Token};

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
    fn test_savepoint_info_serde_json() {
        let success = json!({
            "status": { "id": "COMPLETED" },
            "operation": { "location": "s3a://dev-flink-58dz/foo/savepoints/savepoint-957152-e82cbb4804b1"}
        });
        let success_rep = assert_ok!(serde_json::to_string(&success));

        let actual: query::SavepointInfoResponseBody = assert_ok!(serde_json::from_str(&success_rep));
        assert_eq!(
            actual,
            query::SavepointInfoResponseBody {
                status: QueueStatus { id: QueueState::Completed },
                operation: SavepointOperation {
                    location: Some("s3a://dev-flink-58dz/foo/savepoints/savepoint-957152-e82cbb4804b1".to_string()),
                    failure_reason: None,
                },
            }
        )
    }
}
