use crate::flink::{FlinkContext, JarId, JobId, JobSavepointReport};
use crate::phases::act::action::{ActionSession, ScaleAction};
use crate::phases::act::scale_actuator::ScaleActionPlan;
use crate::phases::act::ActError;
use crate::settings::FlinkActionSettings;
use async_trait::async_trait;
use once_cell::sync::Lazy;
use proctor::AppData;
use prometheus::{HistogramOpts, HistogramTimer, HistogramVec};
use std::time::Duration;

pub const ACTION_LABEL: &str = "restart_jobs";

#[derive(Debug, Clone)]
pub struct RestartJobs<P> {
    pub flink: FlinkContext,
    pub restart_timeout: Duration,
    pub polling_interval: Duration,
    marker: std::marker::PhantomData<P>,
}

impl<P> RestartJobs<P> {
    pub const fn from_settings(flink: FlinkContext, settings: &FlinkActionSettings) -> Self {
        let polling_interval = settings.polling_interval;
        let restart_timeout = settings.restart_operation_timeout;
        Self {
            flink,
            restart_timeout,
            polling_interval,
            marker: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<P> ScaleAction<P> for RestartJobs<P>
where
    P: AppData + ScaleActionPlan,
{
    #[tracing::instrument(level = "info", name = "RestartFlinkWithNewParallelism::execute", skip(self))]
    async fn execute(
        &mut self, mut session: ActionSession<P>,
    ) -> Result<ActionSession<P>, (ActError, ActionSession<P>)> {
        let timer = start_flink_restart_job_timer(&self.flink);
        let correlation = session.correlation();

        let parallelism = session.plan.target_replicas();

        if let Some((active_jobs, savepoints)) = Self::extract_data_from(&session) {
            todo!()
        }
        // let (active_jobs, savepoints) = match (session.active_jobs, session.savepoints.as_ref()) {
        //     (None, None) => {
        //         tracing::warn!(?session, "No active jobs and no savepoints found in session to restart - skipping {ACTION_LABEL}.");
        //         return Ok(session);
        //     },
        //     (None, _) => {},
        // };
        //
        // let active_jobs = match session.active_jobs {
        //     None => {
        //         tracing::warn!(?session, "No active jobs found in session to restart - skipping {ACTION_LABEL}.");
        //         return Ok(session);
        //     },
        //     Some(empty) if empty.is_empty() => {
        //         tracing::warn!(?session, "Empty active jobs list found in session; nothing to restart -- skipping {ACTION_LABEL}");;
        //         return Ok(session);
        //     },
        //     Some(jobs) => jobs,
        // };

        // ---- query for jar id
        // let jars = self.query_uploaded_jars(&correlation).await?;

        // let savepoints = session.savepoints
        // ---- for each job-location
        //   ---- for each jar_id restart with parallelism
        //      --- HTTP 200: grab the response's job id and move onto next location
        //      --- HTTP 400: try the next jar-id;
        //      --- if no jars succeed for the location, track & log the error, and move onto next location

        // ---- for all started job_ids confirm (with budget) that they are running

        todo!()
    }
}

impl<P> RestartJobs<P>
where
    P: AppData + ScaleActionPlan,
{
    #[allow(clippy::cognitive_complexity)]
    fn extract_data_from(session: &ActionSession<P>) -> Option<(&[JobId], &JobSavepointReport)> {
        match (&session.active_jobs, session.savepoints.as_ref()) {
            //todo reconsider in cluster vs standalone support
            (None, None) => {
                tracing::warn!(
                    ?session,
                    "No active jobs and no savepoints found in session to restart - skipping {ACTION_LABEL}."
                );
                None
            },
            //todo reconsider in cluster vs standalone support
            (None, _) => {
                tracing::warn!(
                    ?session,
                    "No active jobs found in session to restart - skipping {ACTION_LABEL}."
                );
                None
            },
            //todo reconsider in cluster vs standalone support
            (_, None) => {
                tracing::warn!(
                    ?session,
                    "No savepoints found in session to restart - skipping {ACTION_LABEL}."
                );
                None
            },
            //todo reconsider in cluster vs standalone support
            (Some(jobs), Some(savepoints)) if jobs.is_empty() && savepoints.completed.is_empty() => {
                tracing::warn!(?session, "Empty active jobs and empty completed savepoints found in session; nothing to restart -- skipping {ACTION_LABEL}");
                None
            },
            //todo reconsider in cluster vs standalone support
            (Some(jobs), _) if jobs.is_empty() => {
                tracing::warn!(
                    ?session,
                    "Empty active jobs found in session; nothing to restart -- skipping {ACTION_LABEL}"
                );
                None
            },
            //todo reconsider in cluster vs standalone support
            (_, Some(savepoints)) if savepoints.completed.is_empty() => {
                tracing::warn!(
                    ?session,
                    "Empty completed savepoints found in session; nothing to restart -- skipping {ACTION_LABEL}"
                );
                None
            },
            (Some(jobs), Some(savepoints)) => Some((jobs.as_slice(), savepoints)),
        }
    }

    // async fn try_
}

#[inline]
fn start_flink_restart_job_timer(context: &FlinkContext) -> HistogramTimer {
    FLINK_RESTART_JOB_TIME.with_label_values(&[context.label()]).start_timer()
}

pub static FLINK_RESTART_JOB_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "flink_job_savepoint_with_cancel_time_seconds",
            "Time spent waiting for savepoint with cancel to complete",
        )
        .buckets(vec![1., 1.5, 2., 3., 4., 5., 10.0, 25., 50., 75., 100.]),
        &["label"],
    )
    .expect("failed creating flink_restart_job_time histogram metric")
});
