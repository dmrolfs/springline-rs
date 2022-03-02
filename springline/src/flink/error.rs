use either::{Either, Left, Right};
use proctor::elements::{TelemetryType, TelemetryValue};
use proctor::error::MetricLabel;
use proctor::SharedString;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FlinkError {
    #[error("{0}")]
    Url(#[from] proctor::error::UrlError),

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

    #[error("unexpected value: {1} expected: {0}")]
    UnexpectedValue(String, String),
}

impl MetricLabel for FlinkError {
    fn slug(&self) -> SharedString {
        "flink".into()
    }

    fn next(&self) -> Either<SharedString, Box<&dyn MetricLabel>> {
        match self {
            Self::Url(err) => Right(Box::new(err)),
            Self::HttpRequest(_) => Left("http::request".into()),
            Self::HttpMiddleware(_) => Left("http::middleware".into()),
            Self::InvalidRequestHeaderDetail(_) => Left("http::header".into()),
            Self::Json(_) => Left("http::json".into()),
            Self::ExpectedTelemetryType(_, _) => Left("telemetry".into()),
            Self::UnexpectedValue(_, _) => Left("http::unexpected".into()),
        }
    }
}
