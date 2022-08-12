use k8s_openapi::api::apps::v1::{Deployment, StatefulSet};
use kube::api::{Patch, PatchParams};
use kube::{Api, Client};
use serde_json::json;
use tracing::Instrument;

use crate::kubernetes::KubernetesError;
use crate::kubernetes::{error, KubernetesDeployResource};
use crate::CorrelationId;

#[derive(Debug, Clone)]
pub enum DeployApi {
    StatefulSet { name: String, api: Api<StatefulSet> },
    Deployment { name: String, api: Api<Deployment> },
}

impl DeployApi {
    pub fn from_kubernetes_resource(resource: &KubernetesDeployResource, client: Client) -> Self {
        match resource {
            KubernetesDeployResource::StatefulSet { name } => Self::StatefulSet {
                name: name.clone(),
                api: Api::default_namespaced(client),
            },
            // KubernetesWorkloadResource::Deployment(deploy) =>
            // Self::Deployment(Api::default_namespaced(deploy.client.clone())),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Self::StatefulSet { name, .. } => name,
            Self::Deployment { name, .. } => name,
        }
    }

    pub async fn get_scale(
        &self, correlation: &CorrelationId,
    ) -> Result<Option<i32>, KubernetesError> {
        let span =
            tracing::info_span!("Kubernetes Deploy::get_scale", action=%"get_scale", ?correlation);
        let scale = match self {
            Self::StatefulSet { name, api } => api
                .get_scale(name)
                .instrument(span)
                .await
                .map_err(error::convert_kube_error),
            Self::Deployment { name, api } => api
                .get_scale(name)
                .instrument(span)
                .await
                .map_err(error::convert_kube_error),
        };
        let scale = scale.map(|s| s.spec.and_then(|ss| ss.replicas));
        tracing::info!("k8s scale received for {}: {:?}", self.name(), scale);
        scale
    }

    pub async fn patch_scale(
        &self, replicas: usize, correlation: &CorrelationId,
    ) -> Result<Option<i32>, KubernetesError> {
        let span = tracing::info_span!("Kubernetes Deploy::patch_scale", action=%"patch_scale", ?correlation);
        let params = PatchParams::default();
        let spec = json!({ "spec": { "replicas": replicas } });
        let merge = Patch::Merge(&spec);

        let patch = match self {
            Self::StatefulSet { name, api } => api
                .patch_scale(name, &params, &merge)
                .instrument(span)
                .await
                .map_err(error::convert_kube_error),
            Self::Deployment { name, api } => api
                .patch_scale(name, &params, &merge)
                .instrument(span)
                .await
                .map_err(error::convert_kube_error),
        };

        let patched_scale = patch.map(|s| s.spec.and_then(|ss| ss.replicas));
        tracing::info!(
            "k8s scale patched for {}: target:{replicas} patched:{patched_scale:?}",
            self.name()
        );
        patched_scale
    }
}
