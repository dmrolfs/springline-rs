use std::time::Duration;

use async_trait::async_trait;
use proctor::AppData;
use tracing::Instrument;

use super::{ActionSession, ScaleAction};
use crate::phases::act::action::ActionStatus;
use crate::phases::act::{self, ActError, ActErrorDisposition, ScaleActionPlan};

pub const ACTION_LABEL: &str = "composite";

#[derive(Debug)]
pub struct CompositeAction<P> {
    pub actions: Vec<Box<dyn ScaleAction<Plan = P>>>,
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
    pub fn add_action_step(mut self, action: impl ScaleAction<Plan = P> + 'static) -> Self {
        self.actions.push(Box::new(action));
        self
    }
}

#[async_trait]
impl<P> ScaleAction for CompositeAction<P>
where
    P: AppData + ScaleActionPlan,
{
    type Plan = P;

    fn label(&self) -> &str {
        ACTION_LABEL
    }

    fn check_preconditions(&self, _session: &ActionSession) -> Result<(), ActError> {
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "CompositeAction::execute", skip(self))]
    async fn execute<'s>(
        &mut self, plan: &'s Self::Plan, session: &'s mut ActionSession,
    ) -> Result<(), ActError> {
        let timer =
            act::start_scale_action_timer(session.cluster_label(), super::ACTION_TOTAL_DURATION);

        let label = self.label().to_string();
        for action in self.actions.iter_mut() {
            let mut status = ActionStatus::Failure;
            if let Err(err) = action.check_preconditions(session) {
                session.mark_completion(action.label(), status, Duration::ZERO);
                return Err(err);
            }

            let action_label = action.label().to_string();
            let timer = act::start_scale_action_timer(session.cluster_label(), &action_label);

            let execute_outcome = action
                .execute(plan, session)
                .instrument(tracing::info_span!("act::action::composite", action=%action_label))
                .await;

            let outcome = match execute_outcome {
                Err(err) => {
                    let o =
                        Self::handle_error_on_execute(&label, &action_label, err, plan, session)
                            .await;

                    status = o
                        .as_ref()
                        .map(|_| ActionStatus::Recovered)
                        .unwrap_or(ActionStatus::Failure);
                    o
                },
                o => {
                    status = ActionStatus::Success;
                    o
                },
            };

            let duration = Duration::from_secs_f64(timer.stop_and_record());
            session.mark_completion(action.label(), status, duration);

            if let Err(err) = outcome {
                return Err(err);
            }
        }

        let total_duration = Duration::from_secs_f64(timer.stop_and_record());
        session.mark_completion(
            super::ACTION_TOTAL_DURATION,
            ActionStatus::Success,
            total_duration,
        );
        Ok(())
    }
}

impl<P> CompositeAction<P>
where
    P: AppData + ScaleActionPlan,
{
    #[tracing::instrument(level = "warn", skip(plan, session))]
    async fn handle_error_on_execute<'s>(
        label: &str, action: &str, error: ActError, plan: &'s P, session: &'s mut ActionSession,
    ) -> Result<(), ActError> {
        let track = format!("{label}::execute::{action}");
        tracing::error!(
            ?error, ?plan, ?session, correlation=?session.correlation(), %track,
            "unrecoverable error during composite execute action: {action}"
        );
        act::track_act_errors(&track, Some(&error), ActErrorDisposition::Failed, plan);
        Err(error)
    }
}
