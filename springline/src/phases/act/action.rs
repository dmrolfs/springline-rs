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
pub use restart_jobs::FLINK_MISSED_JAR_RESTARTS;
pub use savepoint::TriggerSavepoint;

#[async_trait]
pub trait ScaleAction: Debug + Send + Sync {
    type In: AppData + ScaleActionPlan;
    async fn execute<'s>(&self, plan: &'s Self::In, session: &'s mut ActionSession) -> Result<(), ActError>;
}

pub const ACTION_TOTAL_DURATION: &str = "total_duration";

fn action_step(action: &str, step: &str) -> String {
    format!("{}::{}", action, step)
}

#[derive(Clone)]
pub struct ActionSession {
    pub correlation: CorrelationId,
    pub kube: KubernetesContext,
    pub flink: FlinkContext,
    pub nr_confirmed_rescaled_taskmanagers: Option<usize>,
    pub active_jobs: Option<Vec<JobId>>,
    pub uploaded_jars: Option<Vec<JarId>>,
    pub savepoints: Option<JobSavepointReport>,
    pub durations: HashMap<String, Duration>,
}

impl ActionSession {
    pub fn new(correlation: CorrelationId, kube: KubernetesContext, flink: FlinkContext) -> Self {
        Self {
            correlation,
            kube,
            flink,
            nr_confirmed_rescaled_taskmanagers: None,
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
        let mut debug = f.debug_struct("ActionSession");
        debug
            .field("correlation", &format!("{}", self.correlation))
            .field("durations", &self.durations);

        if let Some(active_jobs) = &self.active_jobs {
            debug.field("active_jobs", &active_jobs);
        }

        if let Some(uploaded_jars) = &self.uploaded_jars {
            debug.field("uploaded_jars", &uploaded_jars);
        }

        if let Some(savepoints) = &self.savepoints {
            debug.field("savepoints", &savepoints);
        }

        if let Some(nr_confirmed_rescaled_taskmanagers) = &self.nr_confirmed_rescaled_taskmanagers {
            debug.field("nr_confirmed_rescaled_taskmanagers", &nr_confirmed_rescaled_taskmanagers);
        }

        debug.finish()
    }
}
