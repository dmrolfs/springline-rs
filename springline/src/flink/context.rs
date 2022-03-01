use crate::flink::{self, model::JobSummary};
use crate::settings::FlinkSettings;
use futures_util::TryFutureExt;
use http::Method;
use proctor::error::SenseError;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::policies::ExponentialBackoff;
use reqwest_retry::RetryTransientMiddleware;
use std::fmt;
use std::sync::Arc;
use tracing::Instrument;
use url::Url;

#[derive(Debug, Clone)]
pub struct FlinkContext {
    inner: Arc<FlinkContextRef>,
}

impl FlinkContext {
    pub fn new(client: ClientWithMiddleware, base_url: Url) -> Result<Self, SenseError> {
        let mut jobs_endpoint = base_url.clone();
        jobs_endpoint
            .path_segments_mut()
            .map_err(|_| SenseError::NotABaseUrl(base_url.clone()))?
            .push("jobs");

        Ok(Self { inner: Arc::new(FlinkContextRef { client, base_url, jobs_endpoint }), })
    }

    pub fn from_settings(settings: &FlinkSettings) -> Result<Self, SenseError> {
        let client = make_http_client(settings)?;
        let base_url = settings.base_url()?;
        Self::new(client, base_url)
    }

    pub fn client(&self) -> &ClientWithMiddleware {
        &self.inner.client
    }

    pub fn base_url(&self) -> Url {
        self.inner.base_url.clone()
    }

    #[tracing::instrument(level = "info", skip(self))]
    pub async fn query_active_jobs(&self) -> Result<Vec<JobSummary>, SenseError> {
        self.inner.query_active_jobs().await
    }
}

fn make_http_client(settings: &FlinkSettings) -> Result<ClientWithMiddleware, SenseError> {
    let headers = settings.header_map()?;

    let client_builder = reqwest::Client::builder()
        .pool_idle_timeout(settings.pool_idle_timeout)
        .default_headers(headers);

    let client_builder = if let Some(pool_max_idle_per_host) = settings.pool_max_idle_per_host {
        client_builder.pool_max_idle_per_host(pool_max_idle_per_host)
    } else {
        client_builder
    };

    let client = client_builder.build()?;

    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(settings.max_retries);
    Ok(ClientBuilder::new(client)
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build())
}

struct FlinkContextRef {
    pub client: ClientWithMiddleware,
    pub base_url: Url,
    pub jobs_endpoint: Url,
}

impl fmt::Debug for FlinkContextRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlinkContextRef")
            .field("base_url", &self.base_url)
            .field("jobs_endpoint", &self.jobs_endpoint)
            .finish()
    }
}

impl FlinkContextRef {
    pub async fn query_active_jobs(&self) -> Result<Vec<JobSummary>, SenseError> {
        let _timer = flink::start_flink_active_jobs_timer();
        let span = tracing::info_span!("query Flink active jobs");

        let result: Result<Vec<JobSummary>, SenseError> = self
            .client
            .request(Method::GET, self.jobs_endpoint.clone())
            .send()
            .map_err(|error| {
                tracing::error!(?error, "failed Flink API job_summary response");
                error.into()
            })
            .and_then(|response| {
                flink::log_response("active jobs", &response);
                response.text().map_err(|err| err.into())
            })
            .instrument(span)
            .await
            // .map_err(|err: reqwest_middleware::Error| err.into())
            .and_then(|body| {
                let result = serde_json::from_str(body.as_str()).map_err(|err| err.into());
                tracing::info!(%body, ?result, "Flink job summary response body");
                result
            })
            .and_then(|jobs_json_value: serde_json::Value| {
                let result = jobs_json_value
                    .get("jobs")
                    .cloned()
                    .map(|json| serde_json::from_value::<Vec<JobSummary>>(json).map_err(|err| err.into()))
                    .unwrap_or_else(|| Ok(Vec::default()));
                tracing::info!(?result, "Flink job summary response json parsing");
                result
            })
            .map(|jobs: Vec<JobSummary>| {
                jobs.into_iter()
                    .filter(|job_summary| {
                        let is_job_active = job_summary.status.is_active();
                        if !is_job_active {
                            tracing::info!(?job_summary, "filtering out job detail since");
                        }
                        is_job_active
                    })
                    .collect()
            });

        match result {
            Ok(jobs) => Ok(jobs),
            Err(err) => {
                tracing::error!(error=?err, "failed to query Flink active jobs");
                flink::track_flink_errors(super::ACTIVE_JOBS, &err);
                Err(err)
            },
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::flink::{JobId, JobState};
    use claim::*;
    use pretty_assertions::assert_eq;
    use reqwest::header::HeaderMap;
    use reqwest_middleware::ClientBuilder;
    use reqwest_retry::policies::ExponentialBackoff;
    use reqwest_retry::RetryTransientMiddleware;
    use serde_json::json;
    use tokio_test::block_on;
    use url::Url;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn context_for(mock_server: &MockServer) -> anyhow::Result<FlinkContext> {
        let client = reqwest::Client::builder().default_headers(HeaderMap::default()).build()?;
        let retry_policy = ExponentialBackoff::builder().build_with_max_retries(2);
        let client = ClientBuilder::new(client)
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();
        let url = format!("{}/", &mock_server.uri());
        // let url = "http://localhost:8081/".to_string();
        let context = FlinkContext::new(client, Url::parse(url.as_str())?)?;
        Ok(context)
    }

    #[test]
    fn test_query_active_jobs() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_query_active_jobs");
        let _ = main_span.enter();

        block_on(async {
            let mock_server = MockServer::start().await;

            let b = json!({
                "jobs": [
                    {
                        "id": "0771e8332dc401d254a140a707169a48",
                        "status": "RUNNING"
                    },
                    {
                        "id": "5226h8332dc401d254a140a707114f93",
                        "status": "CREATED"
                    }
                ]
            });

            let response = ResponseTemplate::new(200).set_body_json(b);

            Mock::given(method("GET"))
                .and(path("/jobs"))
                .respond_with(response)
                .expect(1)
                .mount(&mock_server)
                .await;

            let context = assert_ok!(context_for(&mock_server));

            let actual = assert_ok!(context.query_active_jobs().await);
            assert_eq!(
                actual,
                vec![
                    JobSummary {
                        id: JobId::new("0771e8332dc401d254a140a707169a48"),
                        status: JobState::Running,
                    },
                    JobSummary {
                        id: JobId::new("5226h8332dc401d254a140a707114f93"),
                        status: JobState::Created,
                    },
                ]
            )
        })
    }
}
