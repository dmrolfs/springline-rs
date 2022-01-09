use std::str::FromStr;
use std::time::Duration;

use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DurationSeconds};
use url::Url;

use super::IncompatibleSourceSettingsError;
use crate::phases::collection::flink::MetricOrder;

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

    #[serde(rename = "metrics_initial_delay_secs")]
    #[serde_as(as = "DurationSeconds<u64>")]
    pub metrics_initial_delay: Duration,

    #[serde(rename = "metrics_interval_secs")]
    #[serde_as(as = "DurationSeconds<u64>")]
    pub metrics_interval: Duration,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metric_orders: Vec<MetricOrder>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub headers: Vec<(String, String)>,

    #[serde(default = "FlinkSettings::default_max_retries")]
    pub max_retries: u32,
}

impl Default for FlinkSettings {
    fn default() -> Self {
        Self {
            job_manager_uri_scheme: Self::DEFAULT_JOB_MANAGER_SCHEME.to_string(),
            job_manager_host: Self::DEFAULT_JOB_MANAGER_HOST.to_string(),
            job_manager_port: Self::DEFAULT_JOB_MANAGER_PORT,
            metrics_initial_delay: Duration::from_secs(2 * 60),
            metrics_interval: Duration::from_secs(15),
            metric_orders: Vec::default(),
            headers: Vec::default(),
            max_retries: Self::DEFAULT_MAX_RETRIES,
        }
    }
}

impl FlinkSettings {
    const DEFAULT_JOB_MANAGER_HOST: &'static str = "localhost";
    const DEFAULT_JOB_MANAGER_PORT: u16 = 8081;
    const DEFAULT_JOB_MANAGER_SCHEME: &'static str = "http";
    const DEFAULT_MAX_RETRIES: u32 = 3;

    pub fn default_job_manager_scheme() -> String {
        Self::DEFAULT_JOB_MANAGER_SCHEME.to_string()
    }

    pub fn default_job_manager_host() -> String {
        Self::DEFAULT_JOB_MANAGER_HOST.to_string()
    }

    pub fn default_job_manager_port() -> u16 {
        Self::DEFAULT_JOB_MANAGER_PORT
    }

    pub fn default_max_retries() -> u32 {
        Self::DEFAULT_MAX_RETRIES
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

    pub fn header_map(&self) -> Result<HeaderMap, IncompatibleSourceSettingsError> {
        let mut result = HeaderMap::with_capacity(self.headers.len());

        for (k, v) in self.headers.iter() {
            let name = HeaderName::from_str(k.as_str())?;
            let value = HeaderValue::from_str(v.as_str())?;
            result.insert(name, value);
        }

        Ok(result)
    }

    pub fn base_url(&self) -> Result<Url, IncompatibleSourceSettingsError> {
        let url = Url::parse(
            format!(
                "{}://{}:{}/",
                self.job_manager_uri_scheme, self.job_manager_host, self.job_manager_port
            )
            .as_str(),
        )?;

        if url.cannot_be_a_base() {
            return Err(IncompatibleSourceSettingsError::UrlCannotBeBase(url));
        }

        Ok(url)
    }
}

#[cfg(test)]
mod tests {
    use claim::*;
    use pretty_assertions::assert_eq;
    use proctor::elements::telemetry::TelemetryType;

    use super::*;
    use crate::phases::collection::flink::{Aggregation, FlinkScope};

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
    fn test_flink_metric_order_serde() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::debug_span!("test_flink_metric_order_serde");
        let _main_span_guard = main_span.enter();

        let metric_orders = vec![
            MetricOrder {
                scope: FlinkScope::Jobs,
                metric: "uptime".to_string(),
                agg: Aggregation::Max,
                telemetry_path: "health.job_uptime_millis".to_string(),
                telemetry_type: TelemetryType::Integer,
            },
            MetricOrder {
                scope: FlinkScope::Kafka,
                metric: "records-lag-max".to_string(),
                agg: Aggregation::Value,
                telemetry_path: "flow.input_records_lag_max".to_string(),
                telemetry_type: TelemetryType::Integer,
            },
            MetricOrder {
                scope: FlinkScope::TaskManagers,
                metric: "Status.JVM.Memory.Heap.Committed".to_string(),
                agg: Aggregation::Sum,
                telemetry_path: "cluster.task_heap_memory_committed".to_string(),
                telemetry_type: TelemetryType::Float,
            },
        ];

        let actual = assert_ok!(ron::to_string(&metric_orders));
        assert_eq!(
            actual,
            r##"[(Jobs,"uptime",max,"health.job_uptime_millis",Integer),(Kafka,"records-lag-max",value,"flow.input_records_lag_max",Integer),(TaskManagers,"Status.JVM.Memory.Heap.Committed",sum,"cluster.task_heap_memory_committed",Float)]"##
        );

        let hydrated: Vec<MetricOrder> = assert_ok!(ron::from_str(actual.as_str()));
        assert_eq!(hydrated, metric_orders);
    }
}
