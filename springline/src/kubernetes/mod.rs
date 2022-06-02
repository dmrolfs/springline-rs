use std::fmt;

use itertools::Itertools;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

use crate::settings::{KubernetesSettings, LoadKubeConfig};

mod context;
mod deploy;
mod error;

pub use context::{FlinkComponent, KubernetesApiConstraints, KubernetesContext};
pub use deploy::DeployApi;
pub use error::{convert_kube_error, KubernetesError};

// todo: consider representing as enum with Display and AsRef<str>
pub const RUNNING_STATUS: &str = "Running";
// pub const PENDING_STATUS: &str = "Pending";
// pub const SUCCEEDED_STATUS: &str = "Succeeded";
// pub const FAILED_STATUS: &str = "Failed";
pub const UNKNOWN_STATUS: &str = "Unknown";

#[tracing::instrument(level = "info", name = "make kubernetes client")]
async fn make_client(settings: &KubernetesSettings) -> Result<kube::Client, KubernetesError> {
    let config = match &settings.client {
        LoadKubeConfig::Infer => {
            tracing::info!(
                "inferring kubernetes configuration from in-cluster environment or fallback to local kubeconfig."
            );
            kube::Config::infer().await?
        },
        LoadKubeConfig::LocalUrl(url) => {
            tracing::info!(
                ?url,
                "Config kube with only cluster_url, everything thing else is default."
            );
            KubeUrl(url.clone()).try_into().map(kube::Config::new)?
        },
        LoadKubeConfig::ClusterEnv => {
            tracing::info!(
                "configuring kubernetes client from cluster's environment variables, following the standard API \
                 Access from a Pod."
            );
            kube::Config::from_cluster_env()?
        },
        LoadKubeConfig::KubeConfig(options) => {
            tracing::info!("create kubernetes client config from the default local kubeconfig file.");
            kube::Config::from_kubeconfig(&options.clone().into()).await?
        },
        LoadKubeConfig::CustomKubeConfig { kubeconfig, options } => {
            tracing::info!(
                "Configure the kubernetes client with custom kubeconfig, bypassing the normal config parsing."
            );
            kube::Config::from_custom_kubeconfig(kubeconfig.clone().into(), &options.clone().into()).await?
        },
    };

    tracing::info!(
        k8s_cluster_url=%config.cluster_url, default_namespace=%config.default_namespace,
        timeout=?config.timeout, accept_invalid_certs=%config.accept_invalid_certs,
        proxy_url=?config.proxy_url,
        "making kubernetes client using config..."
    );
    kube::Client::try_from(config).map_err(|err| {
        tracing::error!(error=?err, "failed to create kubernetes client.");
        err.into()
    })
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

struct KubeUrl(url::Url);

impl TryFrom<KubeUrl> for http::Uri {
    type Error = KubernetesError;

    fn try_from(url: KubeUrl) -> Result<Self, Self::Error> {
        let scheme = url.0.scheme();

        let mut authority = url.0.username().to_string();
        if let Some(password) = url.0.password() {
            authority.push(':');
            authority.push_str(password);
        }
        if !authority.is_empty() {
            authority.push('@')
        }
        if let Some(host) = url.0.host_str() {
            authority.push_str(host);
        }
        if let Some(port) = url.0.port() {
            authority.push(':');
            authority.push_str(port.to_string().as_str());
        }

        let mut path_and_query = url.0.path().to_string();
        if let Some(query) = url.0.query() {
            path_and_query.push('?');
            path_and_query.push_str(query);
        }

        Self::builder()
            .scheme(scheme)
            .authority(authority)
            .path_and_query(path_and_query)
            .build()
            .map_err(|err| err.into())
    }
}

#[cfg(test)]
mod tests {
    use claim::*;
    use pretty_assertions::assert_eq;
    use serde_test::{assert_tokens, Token};

    use super::*;

    const EXPECTED_REP: &str = "\"statefulset/springline\"";

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

// impl From<KubeUrl> for http::Uri {
//     fn from(url: KubeUrl) -> Self {
//         let scheme = url.0.scheme();
//
//         let mut authority = url.0.username().to_string();
//         if let Some(password) = url.0.password() {
//             authority.push(':');
//             authority.push_str(password);
//         }
//         if !authority.is_empty() {
//             authority.push('@')
//         }
//         if let Some(host) = url.0.host_str() {
//             authority.push_str(host);
//         }
//         if let Some(port) = url.0.port() {
//             authority.push(':');
//             authority.push_str(port.to_string().as_str());
//         }
//
//         let mut path_and_query = url.0.path().to_string();
//         if let Some(query) = url.0.query() {
//             path_and_query.push('?');
//             path_and_query.push_str(query);
//         }
//
//         Self::builder()
//             .scheme(scheme)
//             .authority(authority)
//             .path_and_query(path_and_query)
//             .build()
//             .unwrap()
//     }
// }

// let kube = match k8s_settings.client_config {
// LoadKubeConfig::Infer =>  {
// tracing::info!("inferring kubernetes configuration from in-cluster environment or fallback to
// local kubeconfig."); kube::Client::try_default().await
// },
// LoadKubeConfig::LocalUrl(url) => {
// tracing::info!(?url, "Config kube with only cluster_url, everything thing else is default.")
// kube::Client::try_from(kube::Config::new(url.into()))
// },
// LoadKubeConfig::ClusterEnv => {
// tracing::info!("configuring kubernetes client from cluster's environment variables, following the
// standard API Access from a Pod.") kube::Config::from_cluster_env().and_then(kube::Client::
// try_from) },
// LoadKubeConfig::KubeConfig(options) => {
// tracing::info!("create kuberenetes client config from the default local kubeconfig file.")
// kube::Config::from_kubeconfig(&options.into()).and_then(kube::Client::try_from)
// },
// LoadKubeConfig::CustomKubeConfig { kubeconfig, options } => {
// tracing::info!("Configure the kubernetes client with custom kubeconfig, bypassing the normal
// config parsing."); kube::Config::from_custom_kubeconfig(kubeconfig.into(), &options.into())
// .and_then(|config| kube::Client::try_from)
// }
// }
