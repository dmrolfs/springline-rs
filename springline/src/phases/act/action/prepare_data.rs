use std::marker::PhantomData;

use crate::Env;
use async_trait::async_trait;
use proctor::{AppData, Correlation};
use tracing::Instrument;

use super::{ActionSession, ScaleAction};
use crate::flink::{FlinkError, JarId, JobId};
use crate::phases::act::{self, ActError, ActErrorDisposition};
use crate::phases::plan::ScaleActionPlan;
use crate::settings::FlinkActionSettings;

pub const ACTION_LABEL: &str = "prepare_data";

#[derive(Debug)]
pub struct PrepareData<P> {
    entry_class: Option<String>,
    marker: PhantomData<P>,
}

impl<P> PrepareData<P> {
    pub fn from_settings(settings: &FlinkActionSettings) -> Self {
        Self {
            entry_class: settings.entry_class.clone(),
            marker: PhantomData,
        }
    }
}

#[async_trait]
impl<P> ScaleAction for PrepareData<P>
where
    P: AppData + ScaleActionPlan,
{
    type Plan = P;

    fn label(&self) -> &str {
        ACTION_LABEL
    }

    fn check_preconditions(&self, _session: &Env<ActionSession>) -> Result<(), ActError> {
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "PrepareData::execute", skip(self, plan))]
    async fn execute<'s>(
        &mut self, plan: &'s Self::Plan, session: &'s mut Env<ActionSession>,
    ) -> Result<(), ActError> {
        let correlation = session.correlation().relabel();

        let query_active_jobs = session
            .flink
            .query_active_jobs(&correlation)
            .instrument(tracing::info_span!(
                "act::prepare_data - query_active_jobs",
                ?correlation
            ))
            .await
            .map(|jobs| jobs.into_iter().map(|j| j.id).collect());
        let active_jobs = match query_active_jobs {
            Err(err) => self.handle_error_on_query_active_jobs(err, plan, session).await,
            jobs => jobs,
        }?;

        session.active_jobs = Some(active_jobs);

        let query_jars = session
            .flink
            .query_uploaded_jars(&correlation)
            .instrument(tracing::info_span!(
                "act::prepare_data - query_uploaded_jars",
                ?correlation
            ))
            .await
            .map(|jars| jars.into_iter().map(|j| j.id).collect());
        let jars = match query_jars {
            Err(err) => self.handle_error_on_query_uploaded_jars(err, plan, session).await,
            jars => jars,
        }?;

        // If there is one and only one uploaded jar file, optionally override entry-class on job restart.
        if jars.len() == 1 {
            session.entry_class = self.entry_class.clone();
        }

        session.uploaded_jars = Some(jars);
        Ok(())
    }
}

impl<P> PrepareData<P>
where
    P: AppData + ScaleActionPlan,
{
    #[tracing::instrument(level = "warn", skip(self, plan, session))]
    async fn handle_error_on_query_active_jobs<'s>(
        &self, error: FlinkError, plan: &'s P, session: &'s mut Env<ActionSession>,
    ) -> Result<Vec<JobId>, FlinkError> {
        let track = format!("{}::query_active_jobs", self.label());
        tracing::error!(
            ?error, ?plan, ?session, correlation=%session.correlation(), %track,
            "error on query active jobs"
        );
        act::track_act_errors(&track, Some(&error), ActErrorDisposition::Failed, plan);
        Err(error)
    }

    #[tracing::instrument(level = "warn", skip(self, plan, session))]
    async fn handle_error_on_query_uploaded_jars<'s>(
        &self, error: FlinkError, plan: &'s P, session: &'s mut Env<ActionSession>,
    ) -> Result<Vec<JarId>, FlinkError> {
        let track = format!("{}::query_uploaded_jars", self.label());
        tracing::error!(
            ?error, ?plan, ?session, correlation=%session.correlation(), %track,
            "error on query uploaded jars"
        );
        act::track_act_errors(&track, Some(&error), ActErrorDisposition::Failed, plan);
        Err(error)
    }
}
