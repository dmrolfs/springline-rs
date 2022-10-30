use std::fmt;
use std::sync::Arc;

use futures_util::{FutureExt, TryFutureExt};
use http::Method;
use pretty_snowflake::Id;
use proctor::error::UrlError;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::policies::ExponentialBackoff;
use reqwest_retry::RetryTransientMiddleware;
use tracing::Instrument;
use url::Url;

use crate::flink::error::FlinkError;
use crate::flink::model::{JarSummary, TaskManagerDetail};
use crate::flink::{self, model::JobSummary, JobDetail, JobId};
use crate::settings::FlinkSettings;
use crate::CorrelationId;

#[derive(Debug, Clone)]
pub struct FlinkContext {
    inner: Arc<FlinkContextRef>,
}

impl FlinkContext {
    pub fn new(
        label: impl Into<String>, client: ClientWithMiddleware, base_url: Url,
    ) -> Result<Self, FlinkError> {
        let mut jobs_endpoint = base_url.clone();
        jobs_endpoint
            .path_segments_mut()
            .map_err(|_| UrlError::UrlCannotBeBase(base_url.clone()))?
            .push("jobs");

        let mut jars_endpoint = base_url.clone();
        jars_endpoint
            .path_segments_mut()
            .map_err(|_| UrlError::UrlCannotBeBase(base_url.clone()))?
            .push("jars");

        let mut tm_admin_endpoint = base_url.clone();
        tm_admin_endpoint
            .path_segments_mut()
            .map_err(|_| UrlError::UrlCannotBeBase(base_url.clone()))?
            .push("taskmanagers");

        Ok(Self {
            inner: Arc::new(FlinkContextRef {
                cluster_label: label.into(),
                client,
                base_url,
                jobs_endpoint,
                jars_endpoint,
                tm_admin_endpoint,
            }),
        })
    }

    #[tracing::instrument(level = "info", name = "FlinkContext::from_settings")]
    pub fn from_settings(label: &str, settings: &FlinkSettings) -> Result<Self, FlinkError> {
        let client = make_http_client(settings)?;
        let base_url = settings.base_url()?;
        Self::new(label, client, base_url)
    }

    #[tracing::instrument(level = "info", name = "FlinkContext::check", skip(self), fields(label = %self.inner.cluster_label))]
    pub async fn check(&self) -> Result<(), FlinkError> {
        let result = self.inner.check().await;
        tracing::info!("FlinkContext::check: {:?}", result);
        result
    }

    pub fn label(&self) -> &str {
        self.inner.cluster_label.as_str()
    }

    pub fn client(&self) -> &ClientWithMiddleware {
        &self.inner.client
    }

    pub fn base_url(&self) -> Url {
        self.inner.base_url.clone()
    }

    pub fn jobs_endpoint(&self) -> Url {
        self.inner.jobs_endpoint.clone()
    }

    pub fn jars_endpoint(&self) -> Url {
        self.inner.jars_endpoint.clone()
    }

    pub fn taskmanagers_admin_endpoint(&self) -> Url {
        self.inner.tm_admin_endpoint.clone()
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn query_active_jobs(
        &self, correlation: &CorrelationId,
    ) -> Result<Vec<JobSummary>, FlinkError> {
        self.inner.query_active_jobs(correlation).await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn query_job_details(
        &self, job_id: &JobId, correlation: &CorrelationId,
    ) -> Result<JobDetail, FlinkError> {
        self.inner.query_job_details(job_id, correlation).await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn query_uploaded_jars(
        &self, correlation: &CorrelationId,
    ) -> Result<Vec<JarSummary>, FlinkError> {
        self.inner.query_uploaded_jars(correlation).await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn query_taskmanagers<P>(
        &self, correlation: &Id<P>,
    ) -> Result<TaskManagerDetail, FlinkError> {
        self.inner.query_taskmanagers(correlation).await
    }
}

fn make_http_client(settings: &FlinkSettings) -> Result<ClientWithMiddleware, FlinkError> {
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

    let retry_policy = ExponentialBackoff::builder()
        .retry_bounds(settings.min_retry_interval, settings.max_retry_interval)
        .build_with_max_retries(settings.max_retries);

    Ok(ClientBuilder::new(client)
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build())
}

struct FlinkContextRef {
    /// Label identifying the Flink cluster, possibly populated from the "release" pod label.
    pub cluster_label: String,
    pub client: ClientWithMiddleware,
    pub base_url: Url,
    pub jobs_endpoint: Url,
    pub jars_endpoint: Url,
    pub tm_admin_endpoint: Url,
}

impl fmt::Debug for FlinkContextRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlinkContextRef")
            .field("cluster_label", &self.cluster_label)
            .field("base_url", &self.base_url)
            .field("jobs_endpoint", &self.jobs_endpoint)
            .field("jars_endpoint", &self.jars_endpoint)
            .field("tm_admin_endpoint", &self.tm_admin_endpoint)
            .finish()
    }
}

impl FlinkContextRef {
    pub async fn check(&self) -> Result<(), FlinkError> {
        let corr = CorrelationId::direct(self.cluster_label.as_str(), 123, "flink-check");
        match self.query_active_jobs(&corr).await {
            Ok(jobs) => {
                tracing::info!(
                    "successful flink {} connection - found {} active jobs.",
                    self.cluster_label,
                    jobs.len()
                );
                Ok(())
            },
            Err(err) => {
                tracing::error!("failed flink {} connection: {}", self.cluster_label, err);
                Err(err)
            },
        }
    }

    pub async fn query_uploaded_jars(
        &self, correlation: &CorrelationId,
    ) -> Result<Vec<JarSummary>, FlinkError> {
        let _timer = flink::start_flink_uploaded_jars_timer(self.cluster_label.as_str());

        let result: Result<Vec<JarSummary>, FlinkError> = self
            .client
            .request(Method::GET, self.jars_endpoint.clone())
            .send()
            .map_err(|error| {
                tracing::error!(?error, "failed Flink API jar_summary response");
                error.into()
            })
            .and_then(|response| {
                flink::log_response("uploaded jars", &self.jars_endpoint, &response);
                response.text().map(|body| {
                    body.map_err(|err| err.into()).and_then(|b| {
                        let response = serde_json::from_str(&b);
                        tracing::debug!(body=%b, ?response, "Flink jar summary response body");
                        response
                            .map(|resp: jars_protocal::GetJarsResponse| resp.files)
                            .map_err(|err| err.into())
                    })
                })
            })
            .instrument(tracing::debug_span!(
                "query Flink REST API - uploaded jars",
                ?correlation, jars_endpoint=?self.jars_endpoint,
            ))
            .await;

        flink::track_result(
            "uploaded_jars",
            result,
            "failed to query Flink uploaded jars",
            correlation,
        )
    }

    pub async fn query_active_jobs(
        &self, correlation: &CorrelationId,
    ) -> Result<Vec<JobSummary>, FlinkError> {
        let _timer = flink::start_flink_active_jobs_timer(self.cluster_label.as_str());

        let result: Result<Vec<JobSummary>, FlinkError> = self
            .client
            .request(Method::GET, self.jobs_endpoint.clone())
            .send()
            .map_err(|error| {
                tracing::error!(?error, "failed Flink API job_summary response");
                error.into()
            })
            .and_then(|response| {
                flink::log_response("active jobs", &self.jobs_endpoint, &response);
                response.text().map(|body| {
                    body.map_err(|err| err.into()).and_then(|b| {
                        tracing::debug!(body=%b, "Flink job summary response body");
                        let result = serde_json::from_str(&b).map_err(|err| err.into());
                        tracing::debug!(response=?result, "Flink parsed job summary response json value");
                        result
                    })
                })
            })
            .instrument(tracing::debug_span!("query Flink REST API - active jobs", ?correlation))
            .await
            .and_then(|jobs_json_value: serde_json::Value| {
                let result = jobs_json_value
                    .get("jobs")
                    .cloned()
                    .map(|json| serde_json::from_value::<Vec<JobSummary>>(json).map_err(|err| err.into()))
                    .unwrap_or_else(|| Ok(Vec::default()));
                tracing::debug!(?result, "Flink job summary response json parsing");
                result
            })
            .map(|jobs: Vec<JobSummary>| {
                jobs.into_iter()
                    .filter(|job_summary| {
                        let is_job_active = job_summary.status.is_active();
                        if !is_job_active {
                            tracing::debug!(?job_summary, "filtering out inactive job detail");
                        }
                        is_job_active
                    })
                    .collect()
            });

        flink::track_result(
            "active_jobs",
            result,
            "failed to query Flink active jobs",
            correlation,
        )
    }

    pub async fn query_job_details(
        &self, job_id: &JobId, correlation: &CorrelationId,
    ) -> Result<JobDetail, FlinkError> {
        let mut url = self.jobs_endpoint.clone();
        url.path_segments_mut()
            .map_err(|_| FlinkError::NotABaseUrl(self.jobs_endpoint.clone()))?
            .push(job_id.as_ref());

        let _timer = flink::start_flink_query_job_detail_timer(self.cluster_label.as_str());

        let result: Result<JobDetail, FlinkError> = self
            .client
            .request(Method::GET, url.clone())
            .send()
            .map_err(|error| {
                tracing::error!(?error, "failed Flink REST API - job_detail response");
                error.into()
            })
            .and_then(|response| {
                flink::log_response("job detail", &url, &response);
                response.text().map(|body| {
                    body.map_err(|err| err.into()).and_then(|b| {
                        let result = serde_json::from_str(&b).map_err(|err| err.into());
                        tracing::debug!(body=%b, response=?result, "Flink job detail response body");
                        result
                    })
                })
            })
            .instrument(tracing::debug_span!("query FLink REST API - job detail"))
            .await;

        flink::track_result(
            "job_detail",
            result,
            "failed to query Flink job detail",
            correlation,
        )
    }

    pub async fn query_taskmanagers<C>(
        &self, correlation: &Id<C>,
    ) -> Result<TaskManagerDetail, FlinkError> {
        let _timer = flink::start_flink_query_taskmanager_admin_timer(self.cluster_label.as_str());

        let result: Result<TaskManagerDetail, FlinkError> = self
            .client
            .request(Method::GET, self.tm_admin_endpoint.clone())
            .send()
            .map_err(|error| error.into())
            .and_then(|response| {
                flink::log_response("taskmanager admin response", &self.tm_admin_endpoint, &response);
                response.text().map(|body| {
                    body.map_err(|err| err.into())
                        .and_then(|b| {
                            let result = serde_json::from_str(&b).map_err(|err| err.into());
                            tracing::debug!(body=%b, response=?result, "Flink taskmanager_admin response body");
                            result
                        })
                        .and_then(|resp: serde_json::Value| {
                            // resp["taskmanagers"].as_array().map(|tms| tms.len()).unwrap_or(0)
                            let tms = resp["taskmanagers"].as_array();
                            let nr_taskmanagers = tms.map(|v| v.len()).unwrap_or(0);

                            tms.map(|val| {
                                let (total, free) = val.iter()
                                    .filter_map(|v| {
                                        let slots = v["slotsNumber"].as_u64().and_then(|s| u32::try_from(s).ok());
                                        let free = v["freeSlots"].as_u64().and_then(|s| u32::try_from(s).ok());
                                        let result = slots.zip(free);
                                        if result.is_none() {
                                            tracing::warn!(taskmanager_info=?v, "slot info not available");
                                        }
                                        result
                                    })
                                    .fold(
                                        (0_u32, 0_u32),
                                        |(total, free), (v_slots, v_free)| {
                                            (total + v_slots, free + v_free)
                                        }
                                    );

                                TaskManagerDetail {
                                    nr_taskmanagers,
                                    total_task_slots: total as usize,
                                    free_task_slots: free as usize,
                                }
                            })
                                .ok_or_else(|| FlinkError::Other(anyhow::Error::msg("slot info not available in taskmanager admin API response")))
                            //.map(|tms| tms.len()).unwrap_or(0)
                        })
                })
            })
            .instrument(tracing::debug_span!(
                "query Flink REST API - taskmanager admin telemetry",
                ?correlation
            ))
            .await;

        flink::track_result(
            "taskmanager_admin",
            result,
            "failed to query Flink taskmanager admin",
            correlation,
        )
    }
}

mod jars_protocal {
    use serde::{Deserialize, Serialize};
    use url::Url;

    use crate::flink::model::JarSummary;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct GetJarsResponse {
        #[serde(
            serialize_with = "proctor::serde::serialize_to_str",
            deserialize_with = "proctor::serde::deserialize_from_str"
        )]
        pub address: Url,
        pub files: Vec<JarSummary>,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use claim::*;
    use pretty_assertions::assert_eq;
    use pretty_snowflake::Id;
    use proctor::elements::Timestamp;
    use reqwest::header::HeaderMap;
    use reqwest_middleware::ClientBuilder;
    use reqwest_retry::policies::ExponentialBackoff;
    use reqwest_retry::RetryTransientMiddleware;
    use serde_json::json;
    use serde_test::{assert_tokens, Token};
    use tokio_test::block_on;
    use url::Url;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::flink::model::{JarEntry, JobPlan, JobPlanItem, PlanItemInput};
    use crate::flink::{JarId, JobId, JobState, TaskState, VertexDetail, VertexId};

    fn context_for(mock_server: &MockServer) -> anyhow::Result<FlinkContext> {
        let client = reqwest::Client::builder().default_headers(HeaderMap::default()).build()?;
        let retry_policy = ExponentialBackoff::builder().build_with_max_retries(2);
        let client = ClientBuilder::new(client)
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();
        let url = format!("{}/", &mock_server.uri());
        // let url = "http://localhost:8081/".to_string();
        let context = FlinkContext::new("test_flink", client, Url::parse(url.as_str())?)?;
        Ok(context)
    }

    #[test]
    fn test_query_active_jobs() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_query_active_jobs");
        let _ = main_span.enter();

        let correlation = Id::direct("test_query_active_jobs", 17, "ABC");

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

            let actual = assert_ok!(context.query_active_jobs(&correlation).await);
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

    #[test]
    fn test_jars_response_serde_tokens() {
        let data = jars_protocal::GetJarsResponse {
            address: assert_ok!(Url::parse("http://dr-springline:8081")),
            files: vec![
                JarSummary {
                    id: JarId::new("40a90069-0f69-4c89-b4ba-77ec4e728d23_TopSpeedWindowing.jar"),
                    name: "TopSpeedWindowing.jar".to_string(),
                    uploaded_at: Timestamp::from_secs(1646632951534),
                    entry: vec![JarEntry {
                        name: "org.apache.flink.streaming.examples.windowing.TopSpeedWindowing"
                            .to_string(),
                        description: None,
                    }],
                },
                JarSummary {
                    id: JarId::new(
                        "61fad403-bdc9-4d73-8375-15ee714cc692_flink-tutorials-0.0.1-SNAPSHOT.jar",
                    ),
                    name: "flink-tutorials-0.0.1-SNAPSHOT.jar".to_string(),
                    uploaded_at: Timestamp::from_secs(1646632920247),
                    entry: vec![JarEntry {
                        name: "org.springframework.boot.loader.JarLauncher".to_string(),
                        description: None,
                    }],
                },
            ],
        };

        assert_tokens(
            &data,
            &vec![
                Token::Struct { name: "GetJarsResponse", len: 2 },
                Token::Str("address"),
                Token::Str("http://dr-springline:8081/"),
                Token::Str("files"),
                Token::Seq { len: Some(2) },
                Token::Struct { name: "JarSummary", len: 4 },
                Token::Str("id"),
                Token::NewtypeStruct { name: "JarId" },
                Token::Str("40a90069-0f69-4c89-b4ba-77ec4e728d23_TopSpeedWindowing.jar"),
                Token::Str("name"),
                Token::Str("TopSpeedWindowing.jar"),
                Token::Str("uploaded"),
                Token::I64(1646632951534),
                Token::Str("entry"),
                Token::Seq { len: Some(1) },
                Token::Struct { name: "JarEntry", len: 2 },
                Token::Str("name"),
                Token::Str("org.apache.flink.streaming.examples.windowing.TopSpeedWindowing"),
                Token::Str("description"),
                Token::None,
                Token::StructEnd,
                Token::SeqEnd,
                Token::StructEnd,
                Token::Struct { name: "JarSummary", len: 4 },
                Token::Str("id"),
                Token::NewtypeStruct { name: "JarId" },
                Token::Str(
                    "61fad403-bdc9-4d73-8375-15ee714cc692_flink-tutorials-0.0.1-SNAPSHOT.jar",
                ),
                Token::Str("name"),
                Token::Str("flink-tutorials-0.0.1-SNAPSHOT.jar"),
                Token::Str("uploaded"),
                Token::I64(1646632920247),
                Token::Str("entry"),
                Token::Seq { len: Some(1) },
                Token::Struct { name: "JarEntry", len: 2 },
                Token::Str("name"),
                Token::Str("org.springframework.boot.loader.JarLauncher"),
                Token::Str("description"),
                Token::None,
                Token::StructEnd,
                Token::SeqEnd,
                Token::StructEnd,
                Token::SeqEnd,
                Token::StructEnd,
            ],
        )
    }

    #[test]
    fn test_jars_response_serde_ser_json() {
        let data = jars_protocal::GetJarsResponse {
            address: assert_ok!(Url::parse("http://dr-springline:8081/")),
            files: vec![
                JarSummary {
                    id: JarId::new("40a90069-0f69-4c89-b4ba-77ec4e728d23_TopSpeedWindowing.jar"),
                    name: "TopSpeedWindowing.jar".to_string(),
                    uploaded_at: Timestamp::from_secs(1646632951534),
                    entry: vec![JarEntry {
                        name: "org.apache.flink.streaming.examples.windowing.TopSpeedWindowing"
                            .to_string(),
                        description: None,
                    }],
                },
                JarSummary {
                    id: JarId::new(
                        "61fad403-bdc9-4d73-8375-15ee714cc692_flink-tutorials-0.0.1-SNAPSHOT.jar",
                    ),
                    name: "flink-tutorials-0.0.1-SNAPSHOT.jar".to_string(),
                    uploaded_at: Timestamp::from_secs(1646632920247),
                    entry: vec![JarEntry {
                        name: "org.springframework.boot.loader.JarLauncher".to_string(),
                        description: None,
                    }],
                },
            ],
        };

        let actual: serde_json::Value = assert_ok!(serde_json::to_value(data));

        let expected = json!({
            "address": "http://dr-springline:8081/",
            "files": [
                {
                    "id": "40a90069-0f69-4c89-b4ba-77ec4e728d23_TopSpeedWindowing.jar",
                    "name": "TopSpeedWindowing.jar",
                    "uploaded": 1646632951534_i64,
                    "entry": [
                        {
                            "name": "org.apache.flink.streaming.examples.windowing.TopSpeedWindowing",
                            "description": null
                        }
                    ]
                },
                {
                    "id": "61fad403-bdc9-4d73-8375-15ee714cc692_flink-tutorials-0.0.1-SNAPSHOT.jar",
                    "name": "flink-tutorials-0.0.1-SNAPSHOT.jar",
                    "uploaded": 1646632920247_i64,
                    "entry": [
                        {
                            "name": "org.springframework.boot.loader.JarLauncher",
                            "description": null
                        }
                    ]
                }
            ]
        });

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_jars_response_serde_de_json() {
        let body = json!({
            "address": "http://dr-springline:8081",
            "files": [
                {
                    "id": "40a90069-0f69-4c89-b4ba-77ec4e728d23_TopSpeedWindowing.jar",
                    "name": "TopSpeedWindowing.jar",
                    "uploaded": 1646632951534_i64,
                    "entry": [
                        {
                            "name": "org.apache.flink.streaming.examples.windowing.TopSpeedWindowing",
                            "description": null
                        }
                    ]
                },
                {
                    "id": "61fad403-bdc9-4d73-8375-15ee714cc692_flink-tutorials-0.0.1-SNAPSHOT.jar",
                    "name": "flink-tutorials-0.0.1-SNAPSHOT.jar",
                    "uploaded": 1646632920247_i64,
                    "entry": [
                        {
                            "name": "org.springframework.boot.loader.JarLauncher",
                            "description": null
                        }
                    ]
                }
            ]
        });

        let actual: jars_protocal::GetJarsResponse = assert_ok!(serde_json::from_value(body));

        let expected = jars_protocal::GetJarsResponse {
            address: assert_ok!(Url::parse("http://dr-springline:8081")),
            files: vec![
                JarSummary {
                    id: JarId::new("40a90069-0f69-4c89-b4ba-77ec4e728d23_TopSpeedWindowing.jar"),
                    name: "TopSpeedWindowing.jar".to_string(),
                    uploaded_at: Timestamp::from_secs(1646632951534),
                    entry: vec![JarEntry {
                        name: "org.apache.flink.streaming.examples.windowing.TopSpeedWindowing"
                            .to_string(),
                        description: None,
                    }],
                },
                JarSummary {
                    id: JarId::new(
                        "61fad403-bdc9-4d73-8375-15ee714cc692_flink-tutorials-0.0.1-SNAPSHOT.jar",
                    ),
                    name: "flink-tutorials-0.0.1-SNAPSHOT.jar".to_string(),
                    uploaded_at: Timestamp::from_secs(1646632920247),
                    entry: vec![JarEntry {
                        name: "org.springframework.boot.loader.JarLauncher".to_string(),
                        description: None,
                    }],
                },
            ],
        };

        assert_eq!(actual, expected);
    }

    fn make_job_detail_body_and_expectation() -> (serde_json::Value, JobDetail) {
        let body = json!({
            "jid": "a97b6344d775aafe03e55a8e812d2713",
            "name": "CarTopSpeedWindowingExample",
            "isStoppable": false,
            "state": "RUNNING",
            "start-time": 1639156793312_i64,
            "end-time": -1,
            "duration": 23079,
            "maxParallelism": -1,
            "now": 1639156816391_i64,
            "timestamps": {
                "CREATED": 1639156793320_i64,
                "FAILED": 0,
                "RUNNING": 1639156794159_i64,
                "CANCELED": 0,
                "CANCELLING": 0,
                "SUSPENDED": 0,
                "FAILING": 0,
                "RESTARTING": 0,
                "FINISHED": 0,
                "INITIALIZING": 1639156793312_i64,
                "RECONCILING": 0
            },
            "vertices": [
                {
                    "id": "cbc357ccb763df2852fee8c4fc7d55f2",
                    "name": "Source: Custom Source -> Timestamps/Watermarks",
                    "maxParallelism": 128,
                    "parallelism": 1,
                    "status": "RUNNING",
                    "start-time": 1639156794188_i64,
                    "end-time": -1,
                    "duration": 22203,
                    "tasks": {
                        "SCHEDULED": 0,
                        "FINISHED": 0,
                        "FAILED": 0,
                        "CANCELING": 0,
                        "CANCELED": 0,
                        "DEPLOYING": 0,
                        "RECONCILING": 0,
                        "CREATED": 0,
                        "RUNNING": 1,
                        "INITIALIZING": 0
                    },
                    "metrics": {
                        "read-bytes": 0,
                        "read-bytes-complete": false,
                        "write-bytes": 0,
                        "write-bytes-complete": false,
                        "read-records": 0,
                        "read-records-complete": false,
                        "write-records": 0,
                        "write-records-complete": false
                    }
                },
                {
                    "id": "90bea66de1c231edf33913ecd54406c1",
                    "name": "Window(GlobalWindows(), DeltaTrigger, TimeEvictor, ComparableAggregator, PassThroughWindowFunction) -> Sink: Print to Std. Out",
                    "maxParallelism": 128,
                    "parallelism": 1,
                    "status": "RUNNING",
                    "start-time": 1639156794193_i64,
                    "end-time": -1,
                    "duration": 22198,
                    "tasks": {
                        "SCHEDULED": 0,
                        "FINISHED": 0,
                        "FAILED": 0,
                        "CANCELING": 0,
                        "CANCELED": 0,
                        "DEPLOYING": 0,
                        "RECONCILING": 0,
                        "CREATED": 0,
                        "RUNNING": 1,
                        "INITIALIZING": 0
                    },
                    "metrics": {
                        "read-bytes": 0,
                        "read-bytes-complete": false,
                        "write-bytes": 0,
                        "write-bytes-complete": false,
                        "read-records": 0,
                        "read-records-complete": false,
                        "write-records": 0,
                        "write-records-complete": false
                    }
                }
            ],
            "status-counts": {
                "SCHEDULED": 0,
                "FINISHED": 0,
                "FAILED": 0,
                "CANCELING": 0,
                "CANCELED": 0,
                "DEPLOYING": 0,
                "RECONCILING": 0,
                "CREATED": 0,
                "RUNNING": 2,
                "INITIALIZING": 0
            },
            "plan": {
                "jid": "a97b6344d775aafe03e55a8e812d2713",
                "name": "CarTopSpeedWindowingExample",
                "nodes": [
                    {
                        "id": "90bea66de1c231edf33913ecd54406c1",
                        "parallelism": 1,
                        "operator": "",
                        "operator_strategy": "",
                        "description": "Window(GlobalWindows(), DeltaTrigger, TimeEvictor, ComparableAggregator, PassThroughWindowFunction) -&gt; Sink: Print to Std. Out",
                        "inputs": [
                            {
                                "num": 0,
                                "id": "cbc357ccb763df2852fee8c4fc7d55f2",
                                "ship_strategy": "HASH",
                                "exchange": "pipelined_bounded"
                            }
                        ],
                        "optimizer_properties": {}
                    },
                    {
                        "id": "cbc357ccb763df2852fee8c4fc7d55f2",
                        "parallelism": 1,
                        "operator": "",
                        "operator_strategy": "",
                        "description": "Source: Custom Source -&gt; Timestamps/Watermarks",
                        "optimizer_properties": {}
                    }
                ]
            }
        });

        let jid = JobId::new("a97b6344d775aafe03e55a8e812d2713");
        let vid_1 = VertexId::new("cbc357ccb763df2852fee8c4fc7d55f2");
        let vid_2 = VertexId::new("90bea66de1c231edf33913ecd54406c1");

        let expected = JobDetail {
            jid: jid.clone(),
            name: "CarTopSpeedWindowingExample".to_string(),
            is_stoppable: false,
            state: JobState::Running,
            start_time: Timestamp::new(1639156793, 312_000_000),
            end_time: None,
            duration: Some(Duration::from_millis(23079)),
            max_parallelism: None,
            now: Timestamp::new(1639156816, 391_000_000),
            timestamps: maplit::hashmap! {
                JobState::Created => Timestamp::new(1639156793, 320_000_000),
                JobState::Failed => Timestamp::ZERO,
                JobState::Running => Timestamp::new(1639156794, 159_000_000),
                JobState::Canceled => Timestamp::ZERO,
                JobState::Cancelling => Timestamp::ZERO,
                JobState::Suspended => Timestamp::ZERO,
                JobState::Failing => Timestamp::ZERO,
                JobState::Restarting => Timestamp::ZERO,
                JobState::Finished => Timestamp::ZERO,
                JobState::Initializing => Timestamp::new(1639156793, 312_000_000),
                JobState::Reconciling => Timestamp::ZERO,
            },
            vertices: vec![
                VertexDetail {
                    id: vid_1.clone(),
                    name: "Source: Custom Source -> Timestamps/Watermarks".to_string(),
                    max_parallelism: Some(128),
                    parallelism: 1,
                    status: TaskState::Running,
                    start_time: Timestamp::new(1639156794, 188_000_000),
                    end_time: None,
                    duration: Some(Duration::from_millis(22203)),
                    tasks: maplit::hashmap! {
                        TaskState::Scheduled => 0,
                        TaskState::Finished => 0,
                        TaskState::Failed => 0,
                        TaskState::Canceling => 0,
                        TaskState::Canceled => 0,
                        TaskState::Deploying => 0,
                        TaskState::Reconciling => 0,
                        TaskState::Created => 0,
                        TaskState::Running => 1,
                        TaskState::Initializing => 0,
                    },
                    metrics: maplit::hashmap! {
                        "read-bytes".to_string() => 0_i64.into(),
                        "read-bytes-complete".to_string() => false.into(),
                        "write-bytes".to_string() => 0_i64.into(),
                        "write-bytes-complete".to_string() => false.into(),
                        "read-records".to_string() => 0_i64.into(),
                        "read-records-complete".to_string() => false.into(),
                        "write-records".to_string() => 0_i64.into(),
                        "write-records-complete".to_string() => false.into(),
                    },
                },
                VertexDetail {
                    id: vid_2.clone(),
                    name: "Window(GlobalWindows(), DeltaTrigger, TimeEvictor, ComparableAggregator, \
                           PassThroughWindowFunction) -> Sink: Print to Std. Out"
                        .to_string(),
                    max_parallelism: Some(128),
                    parallelism: 1,
                    status: TaskState::Running,
                    start_time: Timestamp::new(1639156794, 193_000_000),
                    end_time: None,
                    duration: Some(Duration::from_millis(22198)),
                    tasks: maplit::hashmap! {
                        TaskState::Scheduled => 0,
                        TaskState::Finished => 0,
                        TaskState::Failed => 0,
                        TaskState::Canceling => 0,
                        TaskState::Canceled => 0,
                        TaskState::Deploying => 0,
                        TaskState::Reconciling => 0,
                        TaskState::Created => 0,
                        TaskState::Running => 1,
                        TaskState::Initializing => 0,
                    },
                    metrics: maplit::hashmap! {
                        "read-bytes".to_string() => 0_i64.into(),
                        "read-bytes-complete".to_string() => false.into(),
                        "write-bytes".to_string() => 0_i64.into(),
                        "write-bytes-complete".to_string() => false.into(),
                        "read-records".to_string() => 0_i64.into(),
                        "read-records-complete".to_string() => false.into(),
                        "write-records".to_string() => 0_i64.into(),
                        "write-records-complete".to_string() => false.into(),
                    },
                },
            ],
            status_counts: maplit::hashmap! {
                TaskState::Scheduled => 0,
                TaskState::Finished => 0,
                TaskState::Failed => 0,
                TaskState::Canceling => 0,
                TaskState::Canceled => 0,
                TaskState::Deploying => 0,
                TaskState::Reconciling => 0,
                TaskState::Created => 0,
                TaskState::Running => 2,
                TaskState::Initializing => 0,
            },
            plan: JobPlan {
                jid,
                name: "CarTopSpeedWindowingExample".to_string(),
                nodes: vec![
                    JobPlanItem {
                        id: vid_2.clone(),
                        parallelism: 1,
                        operator: "".to_string(),
                        operator_strategy: "".to_string(),
                        description: "Window(GlobalWindows(), DeltaTrigger, TimeEvictor, ComparableAggregator, PassThroughWindowFunction) -&gt; Sink: Print to Std. Out".to_string(),
                        inputs: vec![PlanItemInput {
                            num: 0,
                            id: vid_1.clone(),
                            ship_strategy: "HASH".to_string(),
                            exchange: "pipelined_bounded".to_string(),
                        },],
                        optimizer_properties: HashMap::default(),
                    },
                    JobPlanItem {
                        id: vid_1.clone(),
                        parallelism: 1,
                        operator: "".to_string(),
                        operator_strategy: "".to_string(),
                        description: "Source: Custom Source -&gt; Timestamps/Watermarks".to_string(),
                        inputs: Vec::default(),
                        optimizer_properties: HashMap::default(),
                    },
                ],
            }
        };

        (body, expected)
    }

    #[test]
    fn test_query_job_details() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_query_job_details");
        let _ = main_span.enter();

        block_on(async {
            let mock_server = MockServer::start().await;

            let (b, expected) = make_job_detail_body_and_expectation();

            let response = ResponseTemplate::new(200).set_body_json(b);

            let job_id = "a97b6344d775aafe03e55a8e812d2713".into();

            Mock::given(method("GET"))
                .and(path(format!("/jobs/{job_id}")))
                .respond_with(response)
                .expect(1)
                .mount(&mock_server)
                .await;

            let context = assert_ok!(context_for(&mock_server));
            let correlation = Id::direct("test_query_active_jobs", 17, "ABC");
            let actual = assert_ok!(context.query_job_details(&job_id, &correlation).await);
            assert_eq!(actual, expected);
        })
    }
}
