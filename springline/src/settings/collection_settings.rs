use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};

use proctor::phases::collection::SourceSetting;
use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct CollectionSettings {
    pub flink: FlinkSettings,
    pub sources: HashMap<String, SourceSetting>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct FlinkSettings {
    pub job_manager_host: String,
    pub job_manager_port: u16,
    pub job_metrics: Vec<String>,
    pub task_metrics: Vec<String>,
}

impl Default for FlinkSettings {
    fn default() -> Self {
        Self {
            job_manager_host: "127.0.0.1".to_string(),
            job_manager_port: 8081,
            job_metrics: Vec::default(),
            task_metrics: Vec::default(),
        }
    }
}

impl FlinkSettings {
    pub fn job_manager_url(&self, scheme: impl AsRef<str>) -> Result<Url, url::ParseError> {
        let rep = format!(
            "{}//{}:{}",
            scheme.as_ref(),
            self.job_manager_host,
            self.job_manager_port
        );
        Url::parse(rep.as_str())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use proctor::phases::collection::HttpQuery;
    use reqwest::header::{AUTHORIZATION, CONTENT_LENGTH};
    use reqwest::{Method, Url};
    use serde_test::{assert_tokens, Token};

    use super::*;

    #[test]
    fn test_serde_collection_settings() {
        let settings_csv = CollectionSettings {
            // only doing one pair at a time until *convenient* way to pin order and test is determined
            flink: FlinkSettings {
                job_manager_host: "dr-flink-jm-0".to_string(),
                job_metrics: vec!["Status.JVM.Memory.NonHeap.Committed".to_string(), "uptime".to_string()],
                ..FlinkSettings::default()
            },
            sources: maplit::hashmap! {
                "foo".to_string() => SourceSetting::Csv { path: PathBuf::from("./resources/bar.toml"),},
            },
        };

        assert_tokens(
            &settings_csv,
            &vec![
                Token::Struct { name: "CollectionSettings", len: 2 },
                Token::Str("flink"),
                Token::Struct { name: "FlinkSettings", len: 4 },
                Token::Str("job_manager_host"),
                Token::Str("dr-flink-jm-0"),
                Token::Str("job_manager_port"),
                Token::U16(8081),
                Token::Str("job_metrics"),
                Token::Seq { len: Some(2) },
                Token::Str("Status.JVM.Memory.NonHeap.Committed"),
                Token::Str("uptime"),
                Token::SeqEnd,
                Token::Str("task_metrics"),
                Token::Seq { len: Some(0) },
                Token::SeqEnd,
                Token::StructEnd,
                Token::Str("sources"),
                Token::Map { len: Some(1) },
                Token::Str("foo"),
                Token::Struct { name: "SourceSetting", len: 2 },
                Token::Str("type"),
                Token::Str("csv"),
                Token::Str("path"),
                Token::Str("./resources/bar.toml"),
                Token::StructEnd,
                Token::MapEnd,
                Token::StructEnd,
            ],
        );

        let settings_rest = CollectionSettings {
            flink: FlinkSettings {
                job_manager_host: "dr-flink-jm-0".to_string(),
                job_manager_port: 8081,
                job_metrics: vec!["Status.JVM.Memory.NonHeap.Committed".to_string(), "uptime".to_string()],
                task_metrics: Vec::default(),
            },
            // only doing one pair at a time until *convenient* way to pin order and test is determined
            sources: maplit::hashmap! {
                "foo".to_string() => SourceSetting::RestApi(HttpQuery {
                    interval: Duration::from_secs(7),
                    method: Method::GET,
                    url: Url::parse("https://www.example.com/foobar").unwrap(),
                    headers: vec![
                        (CONTENT_LENGTH.to_string(), 17.to_string()),
                        (AUTHORIZATION.to_string(), "Bearer".to_string()),
                    ],
                    max_retries: 3,
                }),
            },
        };

        assert_tokens(
            &settings_rest,
            &vec![
                Token::Struct { name: "CollectionSettings", len: 2 },
                Token::Str("flink"),
                Token::Struct { name: "FlinkSettings", len: 4 },
                Token::Str("job_manager_host"),
                Token::Str("dr-flink-jm-0"),
                Token::Str("job_manager_port"),
                Token::U16(8081),
                Token::Str("job_metrics"),
                Token::Seq { len: Some(2) },
                Token::Str("Status.JVM.Memory.NonHeap.Committed"),
                Token::Str("uptime"),
                Token::SeqEnd,
                Token::Str("task_metrics"),
                Token::Seq { len: Some(0) },
                Token::SeqEnd,
                Token::StructEnd,
                Token::Str("sources"),
                Token::Map { len: Some(1) },
                Token::Str("foo"),
                Token::Struct { name: "HttpQuery", len: 6 },
                Token::Str("type"),
                Token::Str("rest_api"),
                Token::Str("interval_secs"),
                Token::U64(7),
                Token::Str("method"),
                Token::Str("GET"),
                Token::Str("url"),
                Token::Str("https://www.example.com/foobar"),
                Token::Str("headers"),
                Token::Seq { len: Some(2) },
                Token::Tuple { len: 2 },
                Token::Str(CONTENT_LENGTH.as_str()),
                Token::Str("17"),
                Token::TupleEnd,
                Token::Tuple { len: 2 },
                Token::Str(AUTHORIZATION.as_str()),
                Token::Str("Bearer"),
                Token::TupleEnd,
                Token::SeqEnd,
                Token::Str("max_retries"),
                Token::U32(3),
                Token::StructEnd,
                Token::MapEnd,
                Token::StructEnd,
            ],
        );
    }
}
