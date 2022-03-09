use crate::kubernetes::DeployApi;

use crate::phases::act::ActError;

use k8s_openapi::api::core::v1::Pod;
use kube::api::ListParams;
use kube::Api;

#[derive(Debug, Clone)]
pub struct TaskmanagerContext {
    pub deploy: DeployApi,
    pub pods: Api<Pod>,
    pub params: ListParams,
}

impl TaskmanagerContext {
    pub async fn list_pods(&self) -> Result<Vec<Pod>, ActError> {
        self.pods
            .list(&self.params)
            .await
            .map(|pods| pods.items)
            .map_err(|err| ActError::Kubernetes(err.into()))
    }

    pub async fn list_pods_for_field(&self, field_selector: &str) -> Result<Vec<Pod>, ActError> {
        let params = ListParams {
            field_selector: Some(field_selector.to_string()),
            ..self.params.clone()
        };

        self.pods
            .list(&params)
            .await
            .map(|pods| pods.items)
            .map_err(|err| ActError::Kubernetes(err.into()))
    }
}
