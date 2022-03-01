use crate::phases::act::scale_actuator::ScaleActionPlan;
use crate::phases::act::ActError;
use crate::settings::ActionSettings;
use async_trait::async_trait;
use patch_replicas::PatchReplicas;
use proctor::AppData;
use std::fmt::Debug;
use std::time::Duration;

mod flink_savepoint;
mod patch_replicas;

#[async_trait]
pub trait ScaleAction<P>: Debug + Send + Sync {
    async fn execute(&mut self, plan: &P) -> Result<Duration, ActError>;
}

pub fn make_action<T: AppData + ScaleActionPlan>(
    kube: &kube::Client, settings: &ActionSettings,
) -> Box<dyn ScaleAction<T>> {
    let patch_replicas = PatchReplicas::new(&settings.taskmanagers, kube);
    Box::new(patch_replicas)
}
