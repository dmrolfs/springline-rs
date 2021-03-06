use either::{Either, Left, Right};
use proctor::elements::{TelemetryType, TelemetryValue};
use proctor::error::MetricLabel;
use thiserror::Error;

use crate::flink::{FailureCause, JobId, SavepointStatus};

#[derive(Debug, Error)]
pub enum FlinkError {
    #[error("{0}")]
    Url(#[from] proctor::error::UrlError),

    #[error("supplied flink url cannot be a base to query: {0}")]
    NotABaseUrl(url::Url),

    #[error("Flink REST API call failed: {0}")]
    HttpRequest(#[from] reqwest::Error),

    #[error("error occurred in HTTP middleware calling flink: {0}")]
    HttpMiddleware(#[from] reqwest_middleware::Error),

    #[error("{0}")]
    InvalidRequestHeaderDetail(#[source] anyhow::Error),

    #[error("failed processing JSON: {0}")]
    Json(#[from] serde_json::Error),

    #[error("expected telemetry type: {0} but was {1}")]
    ExpectedTelemetryType(TelemetryType, TelemetryValue),

    #[error("unexpected value: {given} but expected: {expected}")]
    UnexpectedValue { expected: String, given: String },

    #[error("Flink left unexpected savepoint condition for job {0}: {1:?}")]
    UnexpectedSavepointStatus(JobId, SavepointStatus),

    #[error("Flink savepoint operation failed for job {job_id}: {cause}")]
    Savepoint { job_id: JobId, cause: FailureCause },

    // #[error("Flink restarts could not be initiated for jars:{jars:?} and savepoint-locations:{locations:?}")]
    // RestartInitiation { jars: Vec<JarId>, locations: Vec<SavepointLocation>, },
    #[error("Flink API operation {0} failed to complete within timeout of {1:?}")]
    Timeout(String, std::time::Duration),
}

impl MetricLabel for FlinkError {
    fn slug(&self) -> String {
        "flink".into()
    }

    fn next(&self) -> Either<String, Box<&dyn MetricLabel>> {
        match self {
            Self::Url(err) => Right(Box::new(err)),
            Self::NotABaseUrl(_) => Left("http::url::NotABaseUrl".into()),
            Self::HttpRequest(_) => Left("http::request".into()),
            Self::HttpMiddleware(_) => Left("http::middleware".into()),
            Self::InvalidRequestHeaderDetail(_) => Left("http::header".into()),
            Self::Json(_) => Left("http::json".into()),
            Self::ExpectedTelemetryType(..) => Left("telemetry".into()),
            Self::UnexpectedValue { .. } => Left("http::unexpected".into()),
            Self::UnexpectedSavepointStatus(..) => Left("savepoint".into()),
            Self::Savepoint { .. } => Left("savepoint".into()),
            Self::Timeout(..) => Left("http::timeout".into()),
        }
    }
}
