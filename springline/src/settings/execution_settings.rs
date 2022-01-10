use itertools::Itertools;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(PartialEq))]
pub struct ExecutionSettings {
    pub k8s_workload_resource: KubernetesWorkloadResource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KubernetesWorkloadResource {
    StatefulSet { name: String },
}

impl KubernetesWorkloadResource {
    pub fn get_name(&self) -> &str {
        match self {
            Self::StatefulSet { name } => name.as_str(),
        }
    }
}

const SEPARATOR: char = '/';
const STATEFUL_SET: &str = "statefulset";

impl Serialize for KubernetesWorkloadResource {
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

impl<'de> Deserialize<'de> for KubernetesWorkloadResource {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Visitor;

        impl<'v> de::Visitor<'v> for Visitor {
            type Value = KubernetesWorkloadResource;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
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
                    STATEFUL_SET => Ok(KubernetesWorkloadResource::StatefulSet { name: name.to_string() }),
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
    fn test_serde_execution_settings_tokens() {
        let settings = ExecutionSettings {
            k8s_workload_resource: KubernetesWorkloadResource::StatefulSet { name: "springline".to_string() },
        };
        assert_tokens(
            &settings,
            &vec![
                Token::Struct { name: "ExecutionSettings", len: 1 },
                Token::Str("k8s_workload_resource"),
                Token::Str("statefulset/springline"),
                Token::StructEnd,
            ],
        );
    }

    #[test]
    fn test_serde_execution_settings() {
        let settings = ExecutionSettings {
            k8s_workload_resource: KubernetesWorkloadResource::StatefulSet { name: "springline".to_string() },
        };

        let json = assert_ok!(serde_json::to_string(&settings));
        assert_eq!(json, format!(r##"{{"k8s_workload_resource":{}}}"##, EXPECTED_REP));

        let ron = assert_ok!(ron::to_string(&settings));
        assert_eq!(ron, format!("(k8s_workload_resource:{})", EXPECTED_REP));
    }

    #[test]
    fn test_kubernetes_workload_resource_serde_tokens() {
        let resource = KubernetesWorkloadResource::StatefulSet { name: "springline".to_string() };
        assert_tokens(&resource, &vec![Token::Str("statefulset/springline")])
    }

    #[test]
    fn test_kubernetes_workload_resource_serde() {
        let resource = KubernetesWorkloadResource::StatefulSet { name: "springline".to_string() };
        let json = assert_ok!(serde_json::to_string(&resource));
        assert_eq!(&json, EXPECTED_REP);

        let ron = assert_ok!(ron::to_string(&resource));
        assert_eq!(&ron, EXPECTED_REP);
    }
}
