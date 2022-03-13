use super::ScaleActionPlan;
use crate::phases::act::ActError;
use std::fmt::{self, Debug};

use async_trait::async_trait;

use crate::flink::{FlinkContext, JarId, JobId, JobSavepointReport};
use crate::model::CorrelationId;

use crate::kubernetes::KubernetesContext;
use proctor::AppData;
use std::collections::HashMap;
use std::time::Duration;

mod composite;
mod patch_replicas;
mod prepare_data;
mod restart_jobs;
mod savepoint;

pub use composite::CompositeAction;
pub use patch_replicas::PatchReplicas;
pub use prepare_data::PrepareData;
pub use restart_jobs::RestartJobs;
pub use savepoint::TriggerSavepoint;

pub use patch_replicas::FLINK_TASKMANAGER_PATCH_REPLICAS_TIME;
pub use restart_jobs::{FLINK_MISSED_JAR_RESTARTS, FLINK_RESTART_JOB_TIME};
pub use savepoint::FLINK_JOB_SAVEPOINT_WITH_CANCEL_TIME;

// pub fn make_action<T: AppData + ScaleActionPlan>(
//     kube: &kube::Client, settings: &ActionSettings,
// ) -> Box<dyn ScaleAction<T>> {
//     let patch_replicas = PatchReplicas::new(patch_replicas::ACTION_LABEL, &settings.taskmanager, kube);
//     Box::new(patch_replicas)
// }

#[async_trait]
pub trait ScaleAction: Debug + Send + Sync {
    type In: AppData + ScaleActionPlan;
    async fn execute<'s>(&self, plan: &'s Self::In, session: &'s mut ActionSession) -> Result<(), ActError>;
}

pub const ACTION_TOTAL_DURATION: &str = "total_duration";

#[derive(Clone)]
pub struct ActionSession {
    // pub plan: ScalePlan,
    pub correlation: CorrelationId,
    pub kube: KubernetesContext,
    pub flink: FlinkContext,
    pub active_jobs: Option<Vec<JobId>>,
    pub uploaded_jars: Option<Vec<JarId>>,
    pub savepoints: Option<JobSavepointReport>,
    pub durations: HashMap<String, Duration>,
}

impl ActionSession {
    // pub fn new(plan: ScalePlan, kube: KubernetesContext, flink: FlinkContext) -> Self {
    pub fn new(correlation: CorrelationId, kube: KubernetesContext, flink: FlinkContext) -> Self {
        Self {
            // plan,
            correlation,
            kube,
            flink,
            active_jobs: None,
            uploaded_jars: None,
            savepoints: None,
            durations: HashMap::new(),
        }
    }

    pub fn cluster_label(&self) -> &str {
        self.flink.label()
    }

    pub fn correlation(&self) -> CorrelationId {
        self.correlation.clone()
    }

    pub fn mark_duration(&mut self, label: impl AsRef<str>, duration: Duration) {
        self.durations.insert(label.as_ref().to_string(), duration);
    }
}

impl Debug for ActionSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActionSession")
            .field("correlation", &format!("{}", self.correlation))
            .field("durations", &self.durations)
            .field("active_jobs", &self.active_jobs)
            .field("uploaded_jars", &self.uploaded_jars)
            .field("savepoints", &self.savepoints)
            .finish()
    }
}
