use std::time::Duration;

use async_trait::async_trait;
use proctor::AppData;
use tracing::Instrument;

use super::{ActionSession, ScaleAction};
use crate::phases::act::action::ActionStatus;
use crate::phases::act::{self, ActError, ActErrorDisposition, ScaleActionPlan};

#[derive(Debug)]
pub struct CompositeAction<P> {
    pub label: String,
    pub actions: Vec<Box<dyn ScaleAction<Plan = P>>>,
}

impl<P> CompositeAction<P>
where
    P: AppData + ScaleActionPlan,
{
    pub fn new(label: impl Into<String>) -> Self {
        Self { label: label.into(), actions: Vec::default() }
    }

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
        self.label.as_str()
    }

    fn check_preconditions(&self, _session: &ActionSession) -> Result<(), ActError> {
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "CompositeAction::execute", skip(self))]
    async fn execute<'s>(
        &mut self, plan: &'s Self::Plan, session: &'s mut ActionSession,
    ) -> Result<(), ActError> {
        let composite_label = self.label().to_string();
        let timer = act::start_rescale_timer(&composite_label);

        for action in self.actions.iter_mut() {
            let action_label = format!("{}::{}", composite_label, action.label());

            let mut status = ActionStatus::Failure;
            if let Err(err) = action.check_preconditions(session) {
                session.mark_completion(action_label, status, Duration::ZERO);
                return Err(err);
            }

            let timer = act::start_rescale_timer(&action_label);

            let execute_outcome = action
                .execute(plan, session)
                .instrument(tracing::info_span!("act::action::composite", action=%action_label))
                .await;

            let outcome = match execute_outcome {
                Err(err) => {
                    let o = Self::handle_error_on_execute(
                        &composite_label,
                        &action_label,
                        err,
                        plan,
                        session,
                    )
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
            session.mark_completion(action_label, status, duration);

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
        label: &str, track: &str, error: ActError, plan: &'s P, session: &'s mut ActionSession,
    ) -> Result<(), ActError> {
        tracing::error!(
            ?error, ?plan, ?session, correlation=?session.correlation(), %track,
            "unrecoverable error during composite execute action: {label}"
        );
        act::track_act_errors(track, Some(&error), ActErrorDisposition::Failed, plan);
        Err(error)
    }
}
