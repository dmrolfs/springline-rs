use std::marker::PhantomData;
use std::time::Duration;

use async_trait::async_trait;
use proctor::AppData;
use tracing::Instrument;

use super::{ActionSession, ScaleAction};
use crate::flink::{JarId, JobId};
use crate::phases::act;
use crate::phases::act::{ActError, ScaleActionPlan};

pub const ACTION_LABEL: &str = "prepare_data";

#[derive(Debug)]
pub struct PrepareData<P> {
    marker: PhantomData<P>,
}

impl<P> Default for PrepareData<P> {
    fn default() -> Self {
        Self { marker: PhantomData }
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

    fn check_preconditions(&self, _session: &ActionSession) -> Result<(), ActError> {
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "PrepareData::execute", skip(self, _plan))]
    async fn execute<'s>(
        &self, _plan: &'s Self::Plan, session: &'s mut ActionSession,
    ) -> Result<(), ActError> {
        let correlation = session.correlation();
        // todo: consider moving this to context channel?? would support keeping track of jar and job?
        let active_jobs: Vec<JobId> = session
            .flink
            .query_active_jobs(&correlation)
            .instrument(tracing::info_span!(
                "act::prepare_data - query_active_jobs",
                ?correlation
            ))
            .await
            .map(|jobs| jobs.into_iter().map(|j| j.id).collect())?;

        session.active_jobs = Some(active_jobs);

        let jars: Vec<JarId> = session
            .flink
            .query_uploaded_jars(&correlation)
            .instrument(tracing::info_span!(
                "act::prepare_data - query_uploaded_jars",
                ?correlation
            ))
            .await
            .map(|jars| jars.into_iter().map(|j| j.id).collect())?;

        session.uploaded_jars = Some(jars);
        Ok(())
    }
}
