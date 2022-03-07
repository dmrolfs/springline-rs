use crate::phases::act::scale_actuator::ScaleActionPlan;
use crate::phases::act::ActError;

use crate::settings::ActionSettings;
use async_trait::async_trait;
use patch_replicas::PatchReplicas;

use proctor::elements::{Telemetry, TelemetryValue};
use proctor::AppData;
use std::collections::HashMap;
use std::fmt::Debug;
use std::time::Duration;
use crate::model::CorrelationId;

mod patch_replicas;
mod savepoint;
mod restart;

#[derive(Debug, Clone, PartialEq)]
pub struct ActionSession<P> {
    pub plan: P,
    pub data: Telemetry,
    pub durations: HashMap<String, Duration>,
}

impl<P> ActionSession<P>
where
    P: AppData + ScaleActionPlan,
{
    pub fn new(plan: P) -> Self {
        Self {
            plan,
            data: Telemetry::default(),
            durations: HashMap::new(),
        }
    }

    pub fn with_data(mut self, key: impl AsRef<str>, value: impl Into<TelemetryValue>) -> Self {
        self.data.insert(key.as_ref().to_string(), value.into());
        self
    }

    pub fn with_duration(mut self, label: impl AsRef<str>, duration: Duration) -> Self {
        self.durations.insert(label.as_ref().to_string(), duration);
        self
    }

    pub fn correlation(&self) -> CorrelationId {
        self.plan.correlation()
    }
}

#[async_trait]
pub trait ScaleAction<P>: Debug + Send + Sync {
    async fn execute(&mut self, session: ActionSession<P>) -> Result<ActionSession<P>, (ActError, ActionSession<P>)>;
}

pub fn make_action<T: AppData + ScaleActionPlan>(
    kube: &kube::Client, settings: &ActionSettings,
) -> Box<dyn ScaleAction<T>> {
    let patch_replicas = PatchReplicas::new(&settings.taskmanager, kube);
    Box::new(patch_replicas)
}
