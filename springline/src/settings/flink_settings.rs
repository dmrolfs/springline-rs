use std::str::FromStr;
use std::time::Duration;

use proctor::error::UrlError;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DurationMilliSeconds, DurationSeconds};
use url::Url;

use crate::flink::FlinkError;

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[cfg_attr(test, derive(PartialEq))]
pub struct FlinkSettings {
    #[serde(default = "FlinkSettings::default_job_manager_scheme")]
    pub job_manager_uri_scheme: String,

    #[serde(default = "FlinkSettings::default_job_manager_host")]
    pub job_manager_host: String,

    #[serde(default = "FlinkSettings::default_job_manager_port")]
    pub job_manager_port: u16,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub headers: Vec<(String, String)>,

    #[serde(default = "FlinkSettings::default_max_retries")]
    pub max_retries: u32,

    #[serde(
        rename = "min_retry_interval_millis",
        default = "FlinkSettings::default_min_retry_interval"
    )]
    #[serde_as(as = "DurationMilliSeconds")]
    pub min_retry_interval: Duration,

    #[serde(
        rename = "max_retry_interval_millis",
        default = "FlinkSettings::default_max_retry_interval"
    )]
    #[serde_as(as = "DurationMilliSeconds")]
    pub max_retry_interval: Duration,

    #[serde(
        default,
        rename = "pool_idle_timeout_secs",
        skip_serializing_if = "Option::is_none"
    )]
    #[serde_as(as = "Option<DurationSeconds>")]
    pub pool_idle_timeout: Option<Duration>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool_max_idle_per_host: Option<usize>,
}

impl Default for FlinkSettings {
    fn default() -> Self {
        Self {
            job_manager_uri_scheme: Self::DEFAULT_JOB_MANAGER_SCHEME.to_string(),
            job_manager_host: Self::DEFAULT_JOB_MANAGER_HOST.to_string(),
            job_manager_port: Self::DEFAULT_JOB_MANAGER_PORT,
            headers: Vec::default(),
            max_retries: Self::DEFAULT_MAX_RETRIES,
            min_retry_interval: Self::default_min_retry_interval(),
            max_retry_interval: Self::default_max_retry_interval(),
            pool_idle_timeout: None,
            pool_max_idle_per_host: None,
        }
    }
}

impl FlinkSettings {
    pub const DEFAULT_JOB_MANAGER_HOST: &'static str = "localhost";
    pub const DEFAULT_JOB_MANAGER_PORT: u16 = 8081;
    pub const DEFAULT_JOB_MANAGER_SCHEME: &'static str = "http";
    const DEFAULT_MAX_RETRIES: u32 = 3;

    pub fn default_job_manager_scheme() -> String {
        Self::DEFAULT_JOB_MANAGER_SCHEME.to_string()
    }

    pub fn default_job_manager_host() -> String {
        Self::DEFAULT_JOB_MANAGER_HOST.to_string()
    }

    pub const fn default_job_manager_port() -> u16 {
        Self::DEFAULT_JOB_MANAGER_PORT
    }

    pub const fn default_max_retries() -> u32 {
        Self::DEFAULT_MAX_RETRIES
    }

    pub const fn default_min_retry_interval() -> Duration {
        Duration::from_secs(1)
    }

    pub const fn default_max_retry_interval() -> Duration {
        Duration::from_secs(5 * 60)
    }

    pub fn job_manager_url(&self, scheme: impl AsRef<str>) -> Result<Url, url::ParseError> {
        let rep = format!(
            "{}//{}:{}",
            scheme.as_ref(),
            self.job_manager_host,
            self.job_manager_port
        );
        Url::parse(rep.as_str())
    }

    pub fn header_map(&self) -> Result<HeaderMap, FlinkError> {
        let mut result = HeaderMap::with_capacity(self.headers.len());

        for (k, v) in self.headers.iter() {
            let name = HeaderName::from_str(k.as_str())
                .map_err(|err| FlinkError::InvalidRequestHeaderDetail(err.into()))?;
            let value = HeaderValue::from_str(v.as_str())
                .map_err(|err| FlinkError::InvalidRequestHeaderDetail(err.into()))?;
            result.insert(name, value);
        }

        Ok(result)
    }

    pub fn base_url(&self) -> Result<Url, UrlError> {
        let url = Url::parse(
            format!(
                "{}://{}:{}/",
                self.job_manager_uri_scheme, self.job_manager_host, self.job_manager_port
            )
            .as_str(),
        )?;

        if url.cannot_be_a_base() {
            return Err(UrlError::UrlCannotBeBase(url));
        }

        Ok(url)
    }
}

#[cfg(test)]
mod tests {
    use claim::*;
    use pretty_assertions::assert_eq;
    use serde_test::{assert_tokens, Token};

    use super::*;

    #[test]
    fn test_flink_settings_default() {
        let actual: FlinkSettings = assert_ok!(ron::from_str("()"));
        assert_eq!(actual, FlinkSettings::default());

        let actual: FlinkSettings = assert_ok!(ron::from_str("(job_manager_port:80)"));
        assert_eq!(
            actual,
            FlinkSettings { job_manager_port: 80, ..FlinkSettings::default() }
        );
    }

    #[test]
    fn test_serde_sensor_settings_2() {
        let settings_rest = FlinkSettings {
            job_manager_uri_scheme: "http".to_string(),
            job_manager_host: "dr-flink-jm-0".to_string(),
            job_manager_port: 8081,
            headers: vec![(
                reqwest::header::AUTHORIZATION.to_string(),
                "foobar".to_string(),
            )],
            max_retries: 5,
            pool_idle_timeout: None,
            pool_max_idle_per_host: None,
            ..FlinkSettings::default()
        };

        assert_tokens(
            &settings_rest,
            &vec![
                Token::Struct { name: "FlinkSettings", len: 7 },
                Token::Str("job_manager_uri_scheme"),
                Token::Str("http"),
                Token::Str("job_manager_host"),
                Token::Str("dr-flink-jm-0"),
                Token::Str("job_manager_port"),
                Token::U16(8081),
                Token::Str("headers"),
                Token::Seq { len: Some(1) },
                Token::Tuple { len: 2 },
                Token::Str("authorization"),
                Token::Str("foobar"),
                Token::TupleEnd,
                Token::SeqEnd,
                Token::Str("max_retries"),
                Token::U32(5),
                Token::Str("min_retry_interval_millis"),
                Token::U64(1_000),
                Token::Str("max_retry_interval_millis"),
                Token::U64(300_000),
                Token::StructEnd,
            ],
        );
    }
}
