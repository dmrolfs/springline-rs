use super::{ActionSession, ScaleAction};
use crate::flink::{JarId, JobId};
use crate::phases::act::ActError;
use crate::phases::plan::ScalePlan;
use async_trait::async_trait;

pub const ACTION_LABEL: &str = "prepare_data";

#[derive(Debug)]
pub struct PrepareData;

#[async_trait]
impl ScaleAction for PrepareData {
    type In = ScalePlan;
    // type Plan = GovernanceOutcome;

    #[tracing::instrument(level = "info", name = "PrepareData::execute", skip(self))]
    async fn execute<'s>(&self, _plan: &'s Self::In, session: &'s mut ActionSession) -> Result<(), ActError> {
        let start = tokio::time::Instant::now();

        let correlation = session.correlation();
        //todo: consider moving this to context channel?? would support keeping track of jar and job?
        let active_jobs: Vec<JobId> = session
            .flink
            .query_active_jobs(&correlation)
            .await
            .map(|jobs| jobs.into_iter().map(|j| j.id).collect())?;

        session.active_jobs = Some(active_jobs);

        let jars: Vec<JarId> = session
            .flink
            .query_uploaded_jars(&correlation)
            .await
            .map(|jars| jars.into_iter().map(|j| j.id).collect())?;

        session.uploaded_jars = Some(jars);
        session.mark_duration(ACTION_LABEL, start.elapsed());
        Ok(())
    }
}
