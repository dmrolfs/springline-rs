use crate::flink::error::FlinkError;
use crate::flink::{self, model::JobSummary, JarId};
use crate::model::{CorrelationId, MetricCatalog};
use crate::settings::FlinkSettings;
use futures_util::TryFutureExt;
use http::Method;
use proctor::error::UrlError;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::policies::ExponentialBackoff;
use reqwest_retry::RetryTransientMiddleware;
use std::fmt;
use std::sync::Arc;
use tracing::Instrument;
use url::Url;
use crate::flink::model::JarSummary;

#[derive(Debug, Clone)]
pub struct FlinkContext {
    inner: Arc<FlinkContextRef>,
}

impl FlinkContext {
    pub fn new(label: impl Into<String>, client: ClientWithMiddleware, base_url: Url) -> Result<Self, FlinkError> {
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

        Ok(Self {
            inner: Arc::new(FlinkContextRef {
                cluster_label: label.into(),
                client,
                base_url,
                jobs_endpoint,
                jars_endpoint,
            }),
        })
    }

    pub fn from_settings(settings: &FlinkSettings) -> Result<Self, FlinkError> {
        let client = make_http_client(settings)?;
        let base_url = settings.base_url()?;
        Self::new(settings.label.as_str(), client, base_url)
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

    #[tracing::instrument(level = "info", skip(self))]
    pub async fn query_active_jobs(&self, correlation: &CorrelationId) -> Result<Vec<JobSummary>, FlinkError> {
        self.inner.query_active_jobs(correlation).await
    }

    #[tracing::instrument(level = "info", skip(self))]
    pub async fn query_uploaded_jars(&self, correlation: &CorrelationId) -> Result<Vec<JarSummary>, FlinkError> {
        self.inner.query_uploaded_jars(correlation).await
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

    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(settings.max_retries);
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
}

impl fmt::Debug for FlinkContextRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlinkContextRef")
            .field("cluster_label", &self.cluster_label)
            .field("base_url", &self.base_url)
            .field("jobs_endpoint", &self.jobs_endpoint)
            .field("jars_endpoint", &self.jars_endpoint)
            .finish()
    }
}

impl FlinkContextRef {
    #[tracing::instrument(level = "info", skip(self))]
    async fn query_uploaded_jars(&self, correlation: &CorrelationId) -> Result<Vec<JarSummary>, FlinkError> {
        let _timer = flink::start_flink_uploaded_jars_timer(self.cluster_label.as_str());
        let span = tracing::info_span!("query Flink uploaded jars", %correlation);

        let result: Result<Vec<JarSummary>, FlinkError> = self
            .client
            .request(Method::GET, self.jars_endpoint.clone())
            .send()
            .map_err(|error| {
                tracing::error!(?error, "failed Flink API jar_summary response");
                error.into()
            })
            .and_then(|response| {
                flink::log_response("uploaded jars", &response);
                response.text().map_err(|err| err.into())
            })
            .instrument(span)
            .await
            .and_then(|body| {
                let response = serde_json::from_str(body.as_str());
                tracing::info!(%body, ?response, "Flink jar summery response body");
                response.map(|resp: jars_protocal::GetJarsResponse| resp.files).map_err(|err| err.into())
            });

        match result {
            Ok(jars) => Ok(jars),
            Err(err) => {
                tracing::error!(error=?err, "failed to query Flink uploaded jars");
                flink::track_flink_errors(super::ACTIVE_JOBS, &err);
                Err(err)
            },
        }
    }

    pub async fn query_active_jobs(&self, correlation: &CorrelationId) -> Result<Vec<JobSummary>, FlinkError> {
        let _timer = flink::start_flink_active_jobs_timer(self.cluster_label.as_str());
        let span = tracing::info_span!("query Flink active jobs", %correlation);

        let result: Result<Vec<JobSummary>, FlinkError> = self
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

mod jars_protocal {
    use serde::{Deserialize, Serialize};
    use url::Url;
    use crate::flink::model::JarSummary;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    use super::*;
    use crate::flink::context::jars_protocal::JarFileInfo;
    use crate::flink::{JobId, JobState};
    use claim::*;
    use pretty_assertions::assert_eq;
    use pretty_snowflake::Id;
    use proctor::elements::Timestamp;
    use reqwest::header::HeaderMap;
    use reqwest_middleware::ClientBuilder;
    use reqwest_retry::policies::ExponentialBackoff;
    use reqwest_retry::RetryTransientMiddleware;
    use serde_json::{json, json_unexpected};
    use serde_test::{assert_tokens, Token};
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
                jars_protocal::JarFileInfo {
                    id: JarId::new("40a90069-0f69-4c89-b4ba-77ec4e728d23_TopSpeedWindowing.jar"),
                    name: "TopSpeedWindowing.jar".to_string(),
                    uploaded: Timestamp::from_secs(1646632951534),
                    entry: vec![jars_protocal::JarEntry {
                        name: "org.apache.flink.streaming.examples.windowing.TopSpeedWindowing".to_string(),
                        description: None,
                    }],
                },
                jars_protocal::JarFileInfo {
                    id: JarId::new("61fad403-bdc9-4d73-8375-15ee714cc692_flink-tutorials-0.0.1-SNAPSHOT.jar"),
                    name: "flink-tutorials-0.0.1-SNAPSHOT.jar".to_string(),
                    uploaded: Timestamp::from_secs(1646632920247),
                    entry: vec![jars_protocal::JarEntry {
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
                Token::Struct { name: "JarFileInfo", len: 4 },
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
                Token::Struct { name: "JarFileInfo", len: 4 },
                Token::Str("id"),
                Token::NewtypeStruct { name: "JarId" },
                Token::Str("61fad403-bdc9-4d73-8375-15ee714cc692_flink-tutorials-0.0.1-SNAPSHOT.jar"),
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
                jars_protocal::JarFileInfo {
                    id: JarId::new("40a90069-0f69-4c89-b4ba-77ec4e728d23_TopSpeedWindowing.jar"),
                    name: "TopSpeedWindowing.jar".to_string(),
                    uploaded: Timestamp::from_secs(1646632951534),
                    entry: vec![jars_protocal::JarEntry {
                        name: "org.apache.flink.streaming.examples.windowing.TopSpeedWindowing".to_string(),
                        description: None,
                    }],
                },
                jars_protocal::JarFileInfo {
                    id: JarId::new("61fad403-bdc9-4d73-8375-15ee714cc692_flink-tutorials-0.0.1-SNAPSHOT.jar"),
                    name: "flink-tutorials-0.0.1-SNAPSHOT.jar".to_string(),
                    uploaded: Timestamp::from_secs(1646632920247),
                    entry: vec![jars_protocal::JarEntry {
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
                jars_protocal::JarFileInfo {
                    id: JarId::new("40a90069-0f69-4c89-b4ba-77ec4e728d23_TopSpeedWindowing.jar"),
                    name: "TopSpeedWindowing.jar".to_string(),
                    uploaded: Timestamp::from_secs(1646632951534),
                    entry: vec![jars_protocal::JarEntry {
                        name: "org.apache.flink.streaming.examples.windowing.TopSpeedWindowing".to_string(),
                        description: None,
                    }],
                },
                jars_protocal::JarFileInfo {
                    id: JarId::new("61fad403-bdc9-4d73-8375-15ee714cc692_flink-tutorials-0.0.1-SNAPSHOT.jar"),
                    name: "flink-tutorials-0.0.1-SNAPSHOT.jar".to_string(),
                    uploaded: Timestamp::from_secs(1646632920247),
                    entry: vec![jars_protocal::JarEntry {
                        name: "org.springframework.boot.loader.JarLauncher".to_string(),
                        description: None,
                    }],
                },
            ],
        };

        assert_eq!(actual, expected);
    }
}
