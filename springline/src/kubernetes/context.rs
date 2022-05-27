use std::fmt::{self, Display};
use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::core::v1::Pod;
use kube::api::ListParams;
use kube::{Api, Client};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use tracing_futures::Instrument;

use super::{DeployApi, KubernetesDeployResource, KubernetesError};
use crate::settings::Settings;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskmanagerSpec {
    /// A selector to identify taskmanagers the list of returned objects by the scaling target;
    /// e.g., "app=flink,component=taskmanager"
    pub label_selector: String,

    /// Resource name of the deployment resource used to deploy taskmanagers;
    /// e.g. "statefulset/my-taskmanager".
    pub deploy_resource: KubernetesDeployResource,

    /// Constraints used when using the kubernetes API to scale the taskmanagers.
    pub kubernetes_api: KubernetesApiConstraints,
}

#[serde_as]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
        Duration::from_secs(290)
    }
}

#[derive(Debug, Display, Copy, Clone, PartialEq, Eq)]
pub enum FlinkComponent {
    TaskManager,
}

#[derive(Debug, Clone)]
pub struct TaskmanagerContext {
    pub deploy: DeployApi,
    // pub pods: Api<Pod>,
    pub params: ListParams,
    pub spec: TaskmanagerSpec,
}

#[derive(Debug, Clone)]
pub struct KubernetesContext {
    inner: Arc<KubernetesContextRef>,
}

impl KubernetesContext {
    pub async fn from_settings(settings: &Settings) -> Result<Self, KubernetesError> {
        let kube = super::make_client(&settings.kubernetes).await?;
        let taskmanager = TaskmanagerSpec {
            label_selector: settings.action.taskmanager.label_selector.clone(),
            deploy_resource: settings.action.taskmanager.deploy_resource.clone(),
            kubernetes_api: settings.action.taskmanager.kubernetes_api,
        };
        Self::new(kube, taskmanager)
    }

    pub fn new(kube: Client, taskmanager: TaskmanagerSpec) -> Result<Self, KubernetesError> {
        Ok(Self {
            inner: Arc::new(KubernetesContextRef::new(kube, taskmanager)?),
        })
    }

    pub async fn check(&self) -> Result<(), KubernetesError> {
        self.inner.check().await
    }

    pub fn deploy_resource(&self) -> &KubernetesDeployResource {
        &self.inner.taskmanager.spec.deploy_resource
    }

    pub fn api_constraints(&self) -> &KubernetesApiConstraints {
        &self.inner.taskmanager.spec.kubernetes_api
    }

    pub fn taskmanager(&self) -> &TaskmanagerContext {
        &self.inner.taskmanager
    }

    pub async fn list_pods(&self, component: FlinkComponent) -> Result<Vec<Pod>, KubernetesError> {
        let params = match component {
            FlinkComponent::TaskManager => &self.inner.taskmanager.params,
        };

        self.inner
            .list_pods(params)
            .instrument(tracing::info_span!("kube::list_pods", ?params))
            .await
    }

    pub async fn list_pods_for_fields(
        &self, params: &ListParams, field_selector: &str,
    ) -> Result<Vec<Pod>, KubernetesError> {
        self.inner
            .list_pods_for_fields(params, field_selector)
            .instrument(tracing::info_span!("kube::list_pods_for_fields", ?params, %field_selector))
            .await
    }
}

#[derive(Clone)]
struct KubernetesContextRef {
    #[allow(dead_code)]
    kube: Client,
    taskmanager: TaskmanagerContext,
    pods: Api<Pod>,
}

impl fmt::Debug for KubernetesContextRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Inner").field("taskmanager", &self.taskmanager).finish()
    }
}

impl KubernetesContextRef {
    pub fn new(kube: Client, spec: TaskmanagerSpec) -> Result<Self, KubernetesError> {
        let taskmanager = TaskmanagerContext {
            deploy: DeployApi::from_kubernetes_resource(&spec.deploy_resource, kube.clone()),
            params: ListParams {
                label_selector: Some(spec.label_selector.clone()),
                timeout: Some(u32::try_from(spec.kubernetes_api.api_timeout.as_secs())?),
                ..Default::default()
            },
            spec,
        };

        let pods = Api::default_namespaced(kube.clone());

        Ok(Self { kube, taskmanager, pods })
    }

    pub async fn check(&self) -> Result<(), KubernetesError> {
        match self.list_pods(&self.taskmanager.params).await {
            Ok(tms) => {
                tracing::info!("successful kubernetes connection - found {} taskmanagers", tms.len());
                Ok(())
            },

            Err(err) => {
                tracing::error!("failed kubernetes connection: {}", err);
                Err(err)
            },
        }
    }

    pub async fn list_pods(&self, params: &ListParams) -> Result<Vec<Pod>, KubernetesError> {
        self.pods
            .list(params)
            .instrument(tracing::info_span!("kube::list_pods", ?params))
            .await
            .map(|pods| pods.items)
            .map_err(|err| err.into())
    }

    pub async fn list_pods_for_fields(
        &self, params: &ListParams, field_selector: &str,
    ) -> Result<Vec<Pod>, KubernetesError> {
        let ps = ListParams {
            field_selector: Some(field_selector.to_string()),
            ..params.clone()
        };
        self.list_pods(&ps).await
    }
}
