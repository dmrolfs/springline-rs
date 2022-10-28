use std::time::Duration;

use crate::Env;
use async_trait::async_trait;
use proctor::{AppData, Correlation};
use tracing::Instrument;

use super::{ActionSession, ScaleAction};
use crate::phases::act::action::ActionStatus;
use crate::phases::act::{self, ActError, ActErrorDisposition};
use crate::phases::plan::ScaleActionPlan;

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

    fn is_leaf(&self) -> bool {
        false
    }

    fn check_preconditions(&self, _session: &Env<ActionSession>) -> Result<(), ActError> {
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "CompositeAction::execute", skip(self))]
    async fn execute<'s>(
        &mut self, plan: &'s Self::Plan, session: &'s mut Env<ActionSession>,
    ) -> Result<(), ActError> {
        let composite_label = self.label().to_string();
        let composite_timer = act::start_rescale_timer(&composite_label);

        let mut final_outcome = Ok(());
        let mut composite_status = ActionStatus::Failure;

        for action in self.actions.iter_mut() {
            let action_label = format!("{}::{}", composite_label, action.label());

            let mut status = ActionStatus::Failure;
            if let Err(err) = action.check_preconditions(session) {
                session.mark_completion(action_label, status, Duration::ZERO, action.is_leaf());
                return Err(err);
            }

            let action_step_timer = act::start_rescale_timer(&action_label);

            let execute_outcome = action
                .execute(plan, session)
                .instrument(
                    tracing::info_span!("act::action::composite::step", action=%action_label),
                )
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

            let action_step_duration = Duration::from_secs_f64(action_step_timer.stop_and_record());
            tracing::info!(
                ?outcome, is_leaf=%action.is_leaf(), action=%action_label, ?status, ?action_step_duration,
                "action exited with status and duration"
            );

            session.mark_completion(
                &action_label,
                status,
                action_step_duration,
                action.is_leaf(),
            );

            match outcome {
                Ok(_) => {
                    composite_status = status;
                },
                Err(err) => {
                    composite_status = ActionStatus::Failure;
                    let track = format!("{}::action_step", self.label());
                    tracing::warn!(error=?err, %track, "failed in composite step: {}", action_label);
                    act::track_act_errors(&track, Some(&err), ActErrorDisposition::Failed, plan);
                    final_outcome = Err(err);
                    break;
                },
            }
        }

        let composite_duration = Duration::from_secs_f64(composite_timer.stop_and_record());
        session.mark_completion(
            composite_label,
            composite_status,
            composite_duration,
            self.is_leaf(),
        );

        final_outcome
    }
}

impl<P> CompositeAction<P>
where
    P: AppData + ScaleActionPlan,
{
    #[tracing::instrument(level = "warn", skip(plan, session))]
    async fn handle_error_on_execute<'s>(
        label: &str, track: &str, error: ActError, plan: &'s P, session: &'s mut Env<ActionSession>,
    ) -> Result<(), ActError> {
        tracing::error!(
            ?error, ?plan, ?session, correlation=?session.correlation(), %track,
            "unrecoverable error during composite execute action: {label}"
        );
        act::track_act_errors(track, Some(&error), ActErrorDisposition::Failed, plan);
        Err(error)
    }
}
