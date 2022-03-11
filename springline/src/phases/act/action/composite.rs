use super::{ActionSession, ScaleAction};
use crate::phases::act::scale_actuator::ScaleActionPlan;
use crate::phases::act::{self, ActError};
use async_trait::async_trait;
use proctor::AppData;
use std::time::Duration;

pub const ACTION_LABEL: &str = "composite";

#[derive(Debug)]
pub struct CompositeAction<P> {
    // pub cluster_label: String,
    pub actions: Vec<Box<dyn ScaleAction<In = P>>>,
    // marker: std::marker::PhantomData<P>,
}

impl<P> Default for CompositeAction<P> {
    fn default() -> Self {
        Self { actions: Vec::default() }
    }
}

impl<P> CompositeAction<P>
where
    P: AppData + ScaleActionPlan,
{
    // pub fn new(cluster_label: impl Into<String>) -> Self {
    //     Self {
    //         // cluster_label: cluster_label.into(),
    //         actions: Vec::new(),
    //         // marker: std::marker::PhantomData,
    //     }
    // }

    pub fn add_action_step(mut self, action: impl ScaleAction<In = <Self as ScaleAction>::In> + 'static) -> Self {
        self.actions.push(Box::new(action));
        self
    }
}

#[async_trait]
impl<P> ScaleAction for CompositeAction<P>
where
    P: AppData + ScaleActionPlan,
{
    type In = P;
    // type Plan = GovernanceOutcome;

    #[tracing::instrument(level = "info", name = "CompositeAction::execute", skip(self))]
    async fn execute<'s>(&self, plan: &'s Self::In, session: &'s mut ActionSession) -> Result<(), ActError> {
        let timer = act::start_scale_action_timer(session.cluster_label());

        for action in self.actions.iter() {
            action.execute(plan, session).await?;
        }

        session.mark_duration(ACTION_LABEL, Duration::from_secs_f64(timer.stop_and_record()));
        Ok(())
    }
}
