use crate::phases::act::ActError;
use crate::settings::KubernetesDeployResource;
use k8s_openapi::api::apps::v1::{Deployment, StatefulSet};
use k8s_openapi::api::core::v1::Pod;
use kube::api::{ListParams, Patch, PatchParams};
use kube::{Api, Client};
use pretty_snowflake::Id;
use serde_json::json;
use tracing_futures::Instrument;
use crate::model::CorrelationId;

pub const RUNNING_STATUS: &str = "Running";
// pub const PENDING_STATUS: &str = "Pending";
// pub const SUCCEEDED_STATUS: &str = "Succeeded";
// pub const FAILED_STATUS: &str = "Failed";
pub const UNKNOWN_STATUS: &str = "Unknown";

pub fn convert_kube_error(error: kube::Error) -> ActError {
    match error {
        kube::Error::Api(response) => response.into(),
        err => err.into(),
    }
}

#[derive(Debug, Clone)]
pub struct TaskmanagerContext {
    pub deploy: DeployApi,
    pub pods: Api<Pod>,
    pub params: ListParams,
}

impl TaskmanagerContext {
    pub async fn list_pods(&self) -> Result<Vec<Pod>, ActError> {
        let pods = self.pods.list(&self.params).await?;
        Ok(pods.items)
    }

    pub async fn list_pods_for_field(&self, field_selector: &str) -> Result<Vec<Pod>, ActError> {
        let params = ListParams {
            field_selector: Some(field_selector.to_string()),
            ..self.params.clone()
        };
        let pods = self.pods.list(&params).await?;
        Ok(pods.items)
    }
}
#[derive(Debug, Clone)]
pub enum DeployApi {
    StatefulSet { name: String, api: Api<StatefulSet> },
    Deployment { name: String, api: Api<Deployment> },
}

impl DeployApi {
    pub fn from_kubernetes_resource(resource: &KubernetesDeployResource, client: &Client) -> Self {
        match resource {
            KubernetesDeployResource::StatefulSet { name } => Self::StatefulSet {
                name: name.clone(),
                api: Api::default_namespaced(client.clone()),
            },
            // KubernetesWorkloadResource::Deployment(deploy) => Self::Deployment(Api::default_namespaced(deploy.client.clone())),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Self::StatefulSet { name, .. } => name,
            Self::Deployment { name, .. } => name,
        }
    }

    pub async fn get_scale(&self, correlation: &CorrelationId) -> Result<Option<i32>, ActError> {
        let span = tracing::info_span!("Kubernetes Admin Server", phase=%"act", action=%"get_scale", %correlation);
        let scale = match self {
            Self::StatefulSet { name, api } => api.get_scale(name).instrument(span).await.map_err(convert_kube_error),
            Self::Deployment { name, api } => api.get_scale(name).instrument(span).await.map_err(convert_kube_error),
        };
        let scale = scale.map(|s| s.spec.and_then(|ss| ss.replicas));
        tracing::info!("k8s scale received for {}: {:?}", self.name(), scale);
        scale
    }

    pub async fn patch_scale(&self, replicas: usize, correlation: &CorrelationId) -> Result<Option<i32>, ActError> {
        let span = tracing::info_span!("Kubernetes Admin Server", phase=%"act", action=%"patch_scale", %correlation);
        let params = PatchParams::default();
        let spec = json!({ "spec": { "replicas": replicas } });
        let merge = Patch::Merge(&spec);

        let patch = match self {
            Self::StatefulSet { name, api } => api
                .patch_scale(name, &params, &merge)
                .instrument(span)
                .await
                .map_err(convert_kube_error),
            Self::Deployment { name, api } => api
                .patch_scale(name, &params, &merge)
                .instrument(span)
                .await
                .map_err(convert_kube_error),
        };

        let patched_scale = patch.map(|s| s.spec.and_then(|ss| ss.replicas));
        tracing::info!(
            "k8s scale patched for {}: target:{replicas} patched:{patched_scale:?}",
            self.name()
        );
        patched_scale
    }
}
