use crate::flink;
use crate::flink::{FlinkContext, FlinkError, JobId};
use crate::phases::act::action::{ActionSession, ScaleAction};
use crate::phases::act::scale_actuator::ScaleActionPlan;
use crate::phases::act::ActError;
use async_trait::async_trait;
use futures_util::TryFutureExt;
use http::Method;
use once_cell::sync::Lazy;
use proctor::error::UrlError;
use proctor::AppData;
use prometheus::{HistogramOpts, HistogramTimer, HistogramVec};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::fmt;
use tracing::Instrument;
use url::Url;

#[derive(Debug, Clone)]
pub struct TriggerSavepoint<P> {
    pub flink: FlinkContext,
    pub savepoint_dir: Option<String>,
    marker: std::marker::PhantomData<P>,
}

impl<P> TriggerSavepoint<P> {
    pub const fn from_settings(flink: FlinkContext, savepoint_dir: Option<String>) -> Self {
        Self {
            flink,
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
        let correlation = session.correlation();
        let active_jobs: Vec<JobId> = self
            .flink
            .query_active_jobs(correlation)
            .await
            .map_err(|err| (err.into(), session))
            .map(|jobs| jobs.into_iter().map(|j| j.id).collect())?;

        for ref job in active_jobs {}

        // session = session.with_data("active_jobs", active_jobs.into());
        todo!()
    }
}

const TRIGGER_SAVEPOINT: &str = "trigger_savepoint";

impl<P> TriggerSavepoint<P>
where
    P: AppData + ScaleActionPlan,
{
    #[tracing::instrument(level = "info", skip(self))]
    async fn trigger_savepoint(
        &self, job_id: &JobId, cancel_job: bool,
    ) -> Result<(TriggerId, HistogramTimer), FlinkError> {
        let url = self.trigger_endpoint_url_for(job_id)?;
        let timer = start_flink_job_savepoint_with_cancel_timer(&self.flink);
        let span = tracing::info_span!("trigger_savepoint", %url);

        let body = SavepointTriggerRequestBody {
            cancel_job,
            ..SavepointTriggerRequestBody::default()
        };

        let trigger_id: Result<TriggerId, FlinkError> = self
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
                flink::log_response(TRIGGER_SAVEPOINT, &response);
                response.text().map_err(|err| err.into())
            })
            .instrument(span)
            .await
            .and_then(|body| {
                let trigger_response: Result<SavepointTriggerResponseBody, FlinkError> =
                    serde_json::from_str(body.as_str()).map_err(|err| err.into());
                tracing::info!(%body, ?trigger_response, "Savepoint triggered");
                trigger_response.map(|r| r.request_id)
            });

        match trigger_id {
            Ok(tid) => Ok((tid, timer)),
            Err(err) => {
                tracing::error!(error=?err, "failed to trigger savepoint in Flink for job {job_id}");
                flink::track_flink_errors(TRIGGER_SAVEPOINT, &err);
                Err(err)
            },
        }
    }

    fn trigger_endpoint_url_for(&self, id: &JobId) -> Result<Url, UrlError> {
        let mut endpoint_url = self.flink.jobs_endpoint();
        endpoint_url
            .path_segments_mut()
            .map_err(|err| UrlError::UrlCannotBeBase(self.flink.jobs_endpoint()))?
            .push(id.as_ref())
            .push("savepoints");
        Ok(endpoint_url)
    }
}

type TriggerId = String;

#[serde_as]
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "kebab-case")]
struct SavepointTriggerRequestBody {
    pub cancel_job: bool,
    #[serde(rename = "formatType", skip_serializing_if = "Option::is_none")]
    #[serde_as(as = "Option<serde_with::DisplayFromStr>")]
    pub format_type: Option<FormatType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_directory: Option<String>,
    #[serde(rename = "triggerId", skip_serializing_if = "Option::is_none")]
    pub trigger_id: Option<String>,
}

#[derive(Debug, Copy, Clone, PartialEq, Serialize)]
enum FormatType {
    Canonical,
    Native,
}

impl fmt::Display for FormatType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                FormatType::Canonical => "CANONICAL",
                FormatType::Native => "NATIVE",
            }
        )
    }
}

impl std::str::FromStr for FormatType {
    type Err = FlinkError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "CANONICAL" => Ok(Self::Canonical),
            "NATIVE" => Ok(Self::Native),
            rep => Err(FlinkError::UnexpectedValue(
                "CANONICAL or NATIVE".to_string(),
                rep.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct SavepointTriggerResponseBody {
    pub request_id: String,
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_test::{assert_tokens, Token};

    #[test]
    fn test_savepoint_body_serde_tokens() {
        let body = SavepointTriggerRequestBody {
            cancel_job: true,
            format_type: Some(FormatType::Canonical),
            target_directory: Some("/tmp/savepoint".to_string()),
            trigger_id: None,
        };

        assert_tokens(
            &body,
            &vec![
                Token::Struct { name: "SavepointTriggerRequestBody", len: 3 },
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
}
