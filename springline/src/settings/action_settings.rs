use itertools::Itertools;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use serde_with::serde_as;
use std::fmt;
use std::time::Duration;

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(PartialEq))]
pub struct ActionSettings {
    /// Timeout for confirmation of scaling actions. Until this timeout expires, springline will poll
    /// the kubernetes API to check if the scaling action has been satisfied.
    #[serde(default, rename = "action_timeout_secs")]
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    pub action_timeout: Duration,

    pub taskmanager: TaskmanagerContext,

    pub flink: FlinkActionSettings,
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskmanagerContext {
    /// A selector to identify taskmanagers the list of returned objects by the scaling target; e.g.,
    /// "app=flink,component=taskmanager"
    pub label_selector: String,

    /// Resource name of the deployment resource used to deploy taskmanagers; e.g. "statefulset/my-taskmanager".
    pub deploy_resource: KubernetesDeployResource,

    /// Constraints used when using the kubernetes API to scale the taskmanagers.
    pub kubernetes_api: KubernetesApiConstraints,
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FlinkActionSettings {
    /// Interval in which Flink asynchronous API is polled. Defaults to 1 second.
    #[serde(
        default = "FlinkActionSettings::default_polling_interval",
        rename = "polling_interval_secs"
    )]
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    pub polling_interval: Duration,

    pub savepoint: SavepointSettings,
}

impl Default for FlinkActionSettings {
    fn default() -> Self {
        Self {
            polling_interval: Self::default_polling_interval(),
            savepoint: SavepointSettings::default(),
        }
    }
}

impl FlinkActionSettings {
    pub const fn default_polling_interval() -> Duration {
        Duration::from_secs(1)
    }
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SavepointSettings {
    /// Optional directory for the triggered savepoint. If not set, the default will the savepoint
    /// directory configured with Flink.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub directory: Option<String>,

    /// Time allowed for a savepoint to complete. Defaults to 10 minutes.
    #[serde(
        default = "SavepointSettings::default_operation_timeout",
        rename = "operation_timeout_secs"
    )]
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    pub operation_timeout: Duration,
}

impl Default for SavepointSettings {
    fn default() -> Self {
        Self {
            directory: None,
            operation_timeout: Self::default_operation_timeout(),
        }
    }
}

impl SavepointSettings {
    pub const fn default_operation_timeout() -> Duration {
        Duration::from_secs(10 * 60)
    }
}

#[serde_as]
#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
pub struct KubernetesApiConstraints {
    /// Timeout for the kubernetes list/watch call.
    ///
    /// This limits the duration of the call, regardless of any activity or inactivity.
    /// If unset, the default used is 290s.
    /// We limit this to 295s due to [inherent watch limitations](https://github.com/kubernetes/kubernetes/issues/6513).
    #[serde(
        default = "KubernetesApiConstraints::default_api_timeout",
        rename = "api_timeout_secs"
    )]
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    pub api_timeout: Duration,

    /// Interval to query kubernetes API for the status of the scaling action.
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(default, rename = "polling_interval_secs")]
    pub polling_interval: Duration,
}

impl KubernetesApiConstraints {
    const fn default_api_timeout() -> Duration {
        Duration::from_secs(295)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum KubernetesDeployResource {
    StatefulSet { name: String },
}

impl KubernetesDeployResource {
    pub fn get_name(&self) -> &str {
        match self {
            Self::StatefulSet { name } => name.as_str(),
        }
    }
}

const SEPARATOR: char = '/';
const STATEFUL_SET: &str = "statefulset";

impl Serialize for KubernetesDeployResource {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let rep = match self {
            Self::StatefulSet { name } => format!("{}{}{}", STATEFUL_SET, SEPARATOR, name),
        };

        serializer.serialize_str(rep.as_str())
    }
}

impl<'de> Deserialize<'de> for KubernetesDeployResource {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Visitor;

        impl<'v> de::Visitor<'v> for Visitor {
            type Value = KubernetesDeployResource;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "a string for the k8s workload resource type/target")
            }

            fn visit_str<E>(self, rep: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let (workload_resource, name) = rep.split(SEPARATOR).collect_tuple().ok_or_else(|| {
                    serde::de::Error::custom(format!(
                        "expected string in form: <<workload-resource-type>/<resource-name>) but got:{}",
                        rep
                    ))
                })?;

                match workload_resource.to_lowercase().as_str() {
                    STATEFUL_SET => Ok(KubernetesDeployResource::StatefulSet { name: name.to_string() }),
                    value => Err(serde::de::Error::custom(format!(
                        "unknown kubernetes workload resource representation: {}",
                        value
                    ))),
                }
            }
        }

        deserializer.deserialize_str(Visitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use claim::*;
    use pretty_assertions::assert_eq;
    use serde_test::{assert_tokens, Token};

    const EXPECTED_REP: &str = "\"statefulset/springline\"";

    #[test]
    fn test_serde_action_settings_tokens() {
        let settings = ActionSettings {
            action_timeout: Duration::from_secs(700),
            taskmanager: TaskmanagerContext {
                label_selector: "app=flink,component=taskmanager".to_string(),
                deploy_resource: KubernetesDeployResource::StatefulSet { name: "springline".to_string() },
                kubernetes_api: KubernetesApiConstraints {
                    api_timeout: Duration::from_secs(285),
                    polling_interval: Duration::from_secs(10),
                },
            },
            flink: FlinkActionSettings {
                savepoint: SavepointSettings {
                    directory: Some("s3a://flink-98dnkj/foo-bar/savepoints".to_string()),
                    ..SavepointSettings::default()
                },
                ..FlinkActionSettings::default()
            },
        };

        assert_tokens(
            &settings,
            &vec![
                Token::Struct { name: "ActionSettings", len: 3 },
                Token::Str("action_timeout_secs"),
                Token::U64(700),
                Token::Str("taskmanager"),
                Token::Struct { name: "TaskmanagerContext", len: 3 },
                Token::Str("label_selector"),
                Token::Str("app=flink,component=taskmanager"),
                Token::Str("deploy_resource"),
                Token::Str("statefulset/springline"),
                Token::Str("kubernetes_api"),
                Token::Struct { name: "KubernetesApiConstraints", len: 2 },
                Token::Str("api_timeout_secs"),
                Token::U64(285),
                Token::Str("polling_interval_secs"),
                Token::U64(10),
                Token::StructEnd,
                Token::StructEnd,
                Token::Str("flink"),
                Token::Struct { name: "FlinkActionSettings", len: 2 },
                Token::Str("polling_interval_secs"),
                Token::U64(1),
                Token::Str("savepoint"),
                Token::Struct { name: "SavepointSettings", len: 2 },
                Token::Str("directory"),
                Token::Some,
                Token::Str("s3a://flink-98dnkj/foo-bar/savepoints"),
                Token::Str("operation_timeout_secs"),
                Token::U64(600),
                Token::StructEnd,
                Token::StructEnd,
                Token::StructEnd,
            ],
        );
    }

    #[test]
    fn test_serde_action_settings() {
        let settings = ActionSettings {
            action_timeout: Duration::from_secs(777),
            taskmanager: TaskmanagerContext {
                label_selector: "app=flink,component=taskmanager".to_string(),
                deploy_resource: KubernetesDeployResource::StatefulSet { name: "springline".to_string() },
                kubernetes_api: KubernetesApiConstraints {
                    api_timeout: Duration::from_secs(275),
                    polling_interval: Duration::from_secs(7),
                },
            },
            flink: FlinkActionSettings {
                savepoint: SavepointSettings {
                    directory: Some("/service_namespace_port/v1/jobs/flink_job_id/savepoints".to_string()),
                    ..SavepointSettings::default()
                },
                ..FlinkActionSettings::default()
            },
        };

        let json = assert_ok!(serde_json::to_string(&settings));
        assert_eq!(
            json,
            format!(
                r##"{{"action_timeout_secs":777,"taskmanager":{{"label_selector":"app=flink,component=taskmanager","deploy_resource":{},"kubernetes_api":{{"api_timeout_secs":275,"polling_interval_secs":7}}}},"flink":{{"polling_interval_secs":1,"savepoint":{{"directory":"/service_namespace_port/v1/jobs/flink_job_id/savepoints","operation_timeout_secs":600}}}}}}"##,
                EXPECTED_REP
            )
        );

        let ron = assert_ok!(ron::to_string(&settings));
        assert_eq!(
            ron,
            format!(
                r##"(action_timeout_secs:777,taskmanager:(label_selector:"app=flink,component=taskmanager",deploy_resource:{},kubernetes_api:(api_timeout_secs:275,polling_interval_secs:7)),flink:(polling_interval_secs:1,savepoint:(directory:Some("/service_namespace_port/v1/jobs/flink_job_id/savepoints"),operation_timeout_secs:600)))"##,
                EXPECTED_REP
            )
        );
    }

    #[test]
    fn test_kubernetes_workload_resource_serde_tokens() {
        let resource = KubernetesDeployResource::StatefulSet { name: "springline".to_string() };
        assert_tokens(&resource, &vec![Token::Str("statefulset/springline")])
    }

    #[test]
    fn test_kubernetes_workload_resource_serde() {
        let resource = KubernetesDeployResource::StatefulSet { name: "springline".to_string() };
        let json = assert_ok!(serde_json::to_string(&resource));
        assert_eq!(&json, EXPECTED_REP);

        let ron = assert_ok!(ron::to_string(&resource));
        assert_eq!(&ron, EXPECTED_REP);
    }
}
