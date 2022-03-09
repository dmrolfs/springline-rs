use crate::flink::{FlinkContext, JarId, JobId};
use crate::phases::act::action::{ActionSession, ScaleAction};
use crate::phases::act::scale_actuator::ScaleActionPlan;
use crate::phases::act::ActError;
use async_trait::async_trait;
use proctor::AppData;

pub const ACTION_LABEL: &str = "prepare_data";

#[derive(Debug, Clone)]
pub struct PrepareData<P> {
    pub flink: FlinkContext,
    marker: std::marker::PhantomData<P>,
}

impl<P> PrepareData<P> {
    pub const fn new(flink: FlinkContext) -> Self {
        Self { flink, marker: std::marker::PhantomData }
    }
}

#[async_trait]
impl<P> ScaleAction<P> for PrepareData<P>
where
    P: AppData + ScaleActionPlan,
{
    #[tracing::instrument(level = "info", name = "PrepareData::execute", skip(self))]
    async fn execute(
        &mut self, mut session: ActionSession<P>,
    ) -> Result<ActionSession<P>, (ActError, ActionSession<P>)> {
        let start = tokio::time::Instant::now();

        let correlation = session.correlation();
        //todo: consider moving this to context channel?? would support keeping track of jar and job?
        let active_jobs: Vec<JobId> = self
            .flink
            .query_active_jobs(&correlation)
            .await
            .map_err(|err| (err.into(), session.clone()))
            .map(|jobs| jobs.into_iter().map(|j| j.id).collect())?;

        session.active_jobs = Some(active_jobs);

        let jars: Vec<JarId> = self
            .flink
            .query_uploaded_jars(&correlation)
            .await
            .map_err(|err| (err.into(), session.clone()))
            .map(|jars| jars.into_iter().map(|j| j.id).collect())?;

        session.uploaded_jars = Some(jars);

        Ok(session.with_duration(ACTION_LABEL, start.elapsed()))
    }
}
