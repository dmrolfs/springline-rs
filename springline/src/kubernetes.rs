use crate::settings::{KubernetesSettings, LoadKubeConfig};
use kube::{Client, Config};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum KubernetesError {
    #[error(transparent)]
    InferConfig(#[from] kube::config::InferConfigError),

    #[error(transparent)]
    InCluster(#[from] kube::config::InClusterError),

    #[error(transparent)]
    KubeConfig(#[from] kube::config::KubeconfigError),

    #[error("failed in kube client request:{0}")]
    Kube(#[from] kube::Error),

    #[error("could not make HTTP URI from kubernetes url: {0}")]
    Http(#[from] http::Error),
}

#[tracing::instrument(level = "info", name = "make kubernetes client")]
pub async fn make_client(settings: &KubernetesSettings) -> Result<Client, KubernetesError> {
    let config = match &settings.client_config {
        LoadKubeConfig::Infer => {
            tracing::info!(
                "inferring kubernetes configuration from in-cluster environment or fallback to local kubeconfig."
            );
            Config::infer().await?
        },
        LoadKubeConfig::LocalUrl(url) => {
            tracing::info!(
                ?url,
                "Config kube with only cluster_url, everything thing else is default."
            );
            KubeUrl(url.clone()).try_into().map(Config::new)?
        },
        LoadKubeConfig::ClusterEnv => {
            tracing::info!("configuring kubernetes client from cluster's environment variables, following the standard API Access from a Pod.");
            Config::from_cluster_env()?
        },
        LoadKubeConfig::KubeConfig(options) => {
            tracing::info!("create kubernetes client config from the default local kubeconfig file.");
            Config::from_kubeconfig(&options.clone().into()).await?
        },
        LoadKubeConfig::CustomKubeConfig { kubeconfig, options } => {
            tracing::info!(
                "Configure the kubernetes client with custom kubeconfig, bypassing the normal config parsing."
            );
            Config::from_custom_kubeconfig(kubeconfig.clone().into(), &options.clone().into()).await?
        },
    };

    tracing::info!(
        k8s_cluster_url=%config.cluster_url, default_namespace=%config.default_namespace,
        timeout=?config.timeout, accept_invalid_certs=%config.accept_invalid_certs,
        proxy_url=?config.proxy_url,
        "making kubernetes client using config..."
    );
    let client = Client::try_from(config).map_err(|err| err.into());
    if let Err(ref error) = client {
        tracing::error!(?error, "failed to make kubernetes client.");
    }
    client
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
// tracing::info!("inferring kubernetes configuration from in-cluster environment or fallback to local kubeconfig.");
// kube::Client::try_default().await
// },
// LoadKubeConfig::LocalUrl(url) => {
// tracing::info!(?url, "Config kube with only cluster_url, everything thing else is default.")
// kube::Client::try_from(kube::Config::new(url.into()))
// },
// LoadKubeConfig::ClusterEnv => {
// tracing::info!("configuring kubernetes client from cluster's environment variables, following the standard API Access from a Pod.")
// kube::Config::from_cluster_env().and_then(kube::Client::try_from)
// },
// LoadKubeConfig::KubeConfig(options) => {
// tracing::info!("create kuberenetes client config from the default local kubeconfig file.")
// kube::Config::from_kubeconfig(&options.into()).and_then(kube::Client::try_from)
// },
// LoadKubeConfig::CustomKubeConfig { kubeconfig, options } => {
// tracing::info!("Configure the kubernetes client with custom kubeconfig, bypassing the normal config parsing.");
// kube::Config::from_custom_kubeconfig(kubeconfig.into(), &options.into())
// .and_then(|config| kube::Client::try_from)
// }
// }
