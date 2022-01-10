use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default)]
#[cfg_attr(test, derive(PartialEq))]
pub struct KubernetesSettings {
    pub client_config: LoadKubeConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(test, derive(PartialEq))]
pub enum LoadKubeConfig {
    /// Infer the configuration from the environment
    ///
    /// Done by attempting to load in-cluster environment variables first, and
    /// then if that fails, trying the local kubeconfig.
    ///
    /// Fails if inference from both sources fails
    Infer,
    /// Construct a new config where only the cluster_url is set by the user. and everything else
    /// receives a default value.
    /// Most likely you want to use Infer to infer the config from the environment.
    LocalUrl(Url),
    /// Create configuration from the cluster's environment variables.
    /// This follows the standard API Access from a Pod  and relies on you having the service
    /// account's token mounted, as well as having given the service account rbac access to do what
    /// you need.
    ClusterEnv,
    /// Create configuration from the default local config file
    /// This will respect the $KUBECONFIG evar, but otherwise default to ~/.kube/config. You can
    /// also customize what context/cluster/user you want to use here, but it will default to the
    /// current-context.
    KubeConfig(KubeConfigOptions),
    /// Create configuration from a Kubeconfig struct
    /// This bypasses kube's normal config parsing to obtain custom functionality.
    CustomKubeConfig {
        kubeconfig: Kubeconfig,
        options: KubeConfigOptions,
    },
}

impl Default for LoadKubeConfig {
    fn default() -> Self {
        Self::Infer
    }
}

/// [`Kubeconfig`] represents information on how to connect to a remote Kubernetes cluster
///
/// Stored in `~/.kube/config` by default, but can be distributed across multiple paths in passed through `KUBECONFIG`.
/// An analogue of the [config type from client-go](https://github.com/kubernetes/client-go/blob/7697067af71046b18e03dbda04e01a5bb17f9809/tools/clientcmd/api/types.go).
///
/// This type (and its children) are exposed primarily for convenience.
///
/// [`Config`][crate::Config] is the __intended__ developer interface to help create a [`Client`][crate::Client],
/// and this will handle the difference between in-cluster deployment and local development.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Kubeconfig(kube::config::Kubeconfig);

#[cfg(test)]
impl std::cmp::PartialEq for Kubeconfig {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind
            && self.api_version == other.api_version
            && self.current_context == other.current_context
    }
}

impl std::ops::Deref for Kubeconfig {
    type Target = kube::config::Kubeconfig;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(PartialEq))]
pub struct KubeConfigOptions {
    /// the named context to load.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    context: Option<String>,
    /// the cluster to load.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cluster: Option<String>,
    /// the user to load.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    user: Option<String>,
}

impl From<KubeConfigOptions> for kube::config::KubeConfigOptions {
    fn from(springline: KubeConfigOptions) -> Self {
        Self {
            context: springline.context,
            cluster: springline.cluster,
            user: springline.user,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use claim::*;
    use pretty_assertions::assert_eq;
    use serde_test::{assert_tokens, Token};

    #[test]
    fn test_load_kube_config_serde() {
        let load = LoadKubeConfig::Infer;
        let json = assert_ok!(serde_json::to_string(&load));
        assert_eq!(&json, "\"infer\"");
        let actual: LoadKubeConfig = assert_ok!(serde_json::from_str(&json));
        assert_eq!(actual, load);
        let ron = assert_ok!(ron::to_string(&load));
        assert_eq!(&ron, "infer");
        let actual: LoadKubeConfig = assert_ok!(ron::from_str(&ron));
        assert_eq!(actual, load);

        let load = LoadKubeConfig::KubeConfig(KubeConfigOptions {
            context: Some("test-context".to_string()),
            cluster: None,
            user: Some("Fred Flintstone".to_string()),
        });
        let json = assert_ok!(serde_json::to_string(&load));
        assert_eq!(
            &json,
            r##"{"kube_config":{"context":"test-context","user":"Fred Flintstone"}}"##
        );
        let actual: LoadKubeConfig = assert_ok!(serde_json::from_str(&json));
        assert_eq!(actual, load);
        let ron = assert_ok!(ron::to_string(&load));
        assert_eq!(
            &ron,
            r##"kube_config((context:Some("test-context"),user:Some("Fred Flintstone")))"##
        );
        let actual: LoadKubeConfig = assert_ok!(ron::from_str(&ron));
        assert_eq!(actual, load);
    }

    #[test]
    fn test_load_kube_config_serde_tokens() {
        let s1 = KubernetesSettings { client_config: LoadKubeConfig::Infer };
        assert_tokens(
            &s1,
            &vec![
                Token::Struct { name: "KubernetesSettings", len: 1 },
                Token::Str("client_config"),
                Token::UnitVariant { name: "LoadKubeConfig", variant: "infer" },
                Token::StructEnd,
            ],
        );

        let s2 = KubernetesSettings {
            client_config: LoadKubeConfig::KubeConfig(KubeConfigOptions {
                context: Some("foo-context".to_string()),
                cluster: Some("cluster-1".to_string()),
                user: None,
            }),
        };
        assert_tokens(
            &s2,
            &vec![
                Token::Struct { name: "KubernetesSettings", len: 1 },
                Token::Str("client_config"),
                Token::NewtypeVariant { name: "LoadKubeConfig", variant: "kube_config" },
                Token::Struct { name: "KubeConfigOptions", len: 2 },
                Token::Str("context"),
                Token::Some,
                Token::Str("foo-context"),
                Token::Str("cluster"),
                Token::Some,
                Token::Str("cluster-1"),
                Token::StructEnd,
                Token::StructEnd,
            ],
        );
    }
}