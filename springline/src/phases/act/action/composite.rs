use std::time::Duration;

use async_trait::async_trait;
use proctor::AppData;
use tracing::Instrument;

use super::{ActionSession, ScaleAction};
use crate::phases::act::{self, ActError, ScaleActionPlan};

pub const ACTION_LABEL: &str = "composite";

#[derive(Debug)]
pub struct CompositeAction<P> {
    pub actions: Vec<Box<dyn ScaleAction<In = P>>>,
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
    pub fn add_action_step(
        mut self, action: impl ScaleAction<In = <Self as ScaleAction>::In> + 'static,
    ) -> Self {
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

    fn label(&self) -> &str {
        ACTION_LABEL
    }

    fn check_preconditions(&self, _session: &ActionSession) -> Result<(), ActError> {
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "CompositeAction::execute", skip(self))]
    async fn execute<'s>(
        &self, plan: &'s Self::In, session: &'s mut ActionSession,
    ) -> Result<(), ActError> {
        let timer =
            act::start_scale_action_timer(session.cluster_label(), super::ACTION_TOTAL_DURATION);

        for action in self.actions.iter() {
            let outcome =
                match action.check_preconditions(session) {
                    Ok(_) => action
                        .execute(plan, session)
                        .instrument(
                            tracing::info_span!("act::composite::action", action=%action.label()),
                        )
                        .await,
                    Err(err) => Err(err),
                };

            if let Err(err) = outcome {
                return Err(err);
            }

            // action.execute(plan, session).await?;
        }

        let total_duration = Duration::from_secs_f64(timer.stop_and_record());
        session.mark_duration(super::ACTION_TOTAL_DURATION, total_duration);
        Ok(())
    }
}
