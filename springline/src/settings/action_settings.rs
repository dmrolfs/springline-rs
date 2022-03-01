use itertools::Itertools;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use serde_with::serde_as;
use std::fmt;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(PartialEq))]
pub struct ActionSettings {
    pub taskmanager: ScaleContext,
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScaleContext {
    /// A selector to identify taskmanagers the list of returned objects by the scaling target; e.g.,
    /// "app=flink,component=taskmanager"
    pub label_selector: String,

    /// Resource name of the deployment resource used to deploy taskmanagers; e.g. "statefulset/my-taskmanager".
    pub deploy_resource: KubernetesDeployResource,

    /// Constraints used when using the kubernetes API to scale the taskmanagers.
    pub kubernetes_api_constraints: KubernetesApiConstraints,
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

    /// Timeout for confirmation of scaling actions. Until this timeout expires, springline will poll
    /// the kubernetes API to check if the scaling action has been satisfied.
    #[serde(default, rename = "action_timeout_secs")]
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    pub action_timeout: Duration,

    /// Interval to query kubernetes API for the status of the scaling action.
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(default, rename = "action_poll_interval_secs")]
    pub action_poll_interval: Duration,
}

impl KubernetesApiConstraints {
    const fn default_api_timeout() -> Duration {
        Duration::from_secs(295)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
            taskmanager: ScaleContext {
                label_selector: "app=flink,component=taskmanager".to_string(),
                deploy_resource: KubernetesDeployResource::StatefulSet { name: "springline".to_string() },
                kubernetes_api_constraints: KubernetesApiConstraints {
                    api_timeout: Duration::from_secs(285),
                    action_timeout: Duration::from_secs(700),
                    action_poll_interval: Duration::from_secs(10),
                },
            },
        };
        assert_tokens(
            &settings,
            &vec![
                Token::Struct { name: "ActionSettings", len: 1 },
                Token::Str("taskmanager"),
                Token::Struct { name: "ScaleContext", len: 3 },
                Token::Str("label_selector"),
                Token::Str("app=flink,component=taskmanager"),
                Token::Str("deploy_resource"),
                Token::Str("statefulset/springline"),
                Token::Str("kubernetes_api_constraints"),
                Token::Struct { name: "KubernetesApiConstraints", len: 3 },
                Token::Str("api_timeout_secs"),
                Token::U64(285),
                Token::Str("action_timeout_secs"),
                Token::U64(700),
                Token::Str("action_poll_interval_secs"),
                Token::U64(10),
                Token::StructEnd,
                Token::StructEnd,
                Token::StructEnd,
            ],
        );
    }

    #[test]
    fn test_serde_action_settings() {
        let settings = ActionSettings {
            taskmanager: ScaleContext {
                label_selector: "app=flink,component=taskmanager".to_string(),
                deploy_resource: KubernetesDeployResource::StatefulSet { name: "springline".to_string() },
                kubernetes_api_constraints: KubernetesApiConstraints {
                    api_timeout: Duration::from_secs(275),
                    action_timeout: Duration::from_secs(777),
                    action_poll_interval: Duration::from_secs(7),
                },
            },
        };

        let json = assert_ok!(serde_json::to_string(&settings));
        assert_eq!(
            json,
            format!(
                r##"{{"taskmanager":{{"label_selector":"app=flink,component=taskmanager","deploy_resource":{},"kubernetes_api_constraints":{{"api_timeout_secs":275,"action_timeout_secs":777,"action_poll_interval_secs":7}}}}}}"##,
                EXPECTED_REP
            )
        );

        let ron = assert_ok!(ron::to_string(&settings));
        assert_eq!(
            ron,
            format!(
                r##"(taskmanager:(label_selector:"app=flink,component=taskmanager",deploy_resource:{},kubernetes_api_constraints:(api_timeout_secs:275,action_timeout_secs:777,action_poll_interval_secs:7)))"##,
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
