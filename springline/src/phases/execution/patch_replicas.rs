use std::fmt;

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use either::{Either, Left, Right};
use proctor::{AppData, ProctorResult, SharedString};
use proctor::graph::{SinkShape, Inlet, Port, PORT_DATA};
use proctor::graph::stage::Stage;
use proctor::error::{MetricLabel, ProctorError};
use crate::phases::governance::GovernanceOutcome;
use thiserror::Error;

const STAGE_NAME: &str = "execute_scaling";

#[derive(Debug, Error)]
pub enum ExecutionPhaseError {
    #[error("failure occurred in the PatchReplicas inlet port: {0}")]
    PortError(#[from] proctor::error::PortError),

    #[error("failure occurred while processing data in the PatchReplicas stage: {0}")]
    StageError(#[from] anyhow::Error),
}

impl MetricLabel for ExecutionPhaseError {
    fn slug(&self) -> SharedString {
        SharedString::Borrowed("execution")
    }

    fn next(&self) -> Either<SharedString, Box<&dyn MetricLabel>> {
        match self {
            Self::PortError(e) => Right(Box::new(e)),
            Self::StageError(_) => Left("stage".into()),
        }
    }
}

pub trait TargetReplicas {
    fn target_replicas(&self) -> u16;
}

impl TargetReplicas for GovernanceOutcome {
    fn target_replicas(&self) -> u16 {
        self.target_nr_task_managers
    }
}


// #[derive(Debug)]
pub struct PatchReplicas<In> {
    inlet: Inlet<In>,
}

impl<In> fmt::Debug for PatchReplicas<In> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PatchReplicas")
            .field("inlet", &self.inlet)
            .finish()
    }
}

impl<In> Default for PatchReplicas<In> {
    fn default() -> Self {
        Self { inlet: Inlet::new(STAGE_NAME, PORT_DATA), }
    }
}

impl<In> SinkShape for PatchReplicas<In> {
    type In = In;

    #[inline]
    fn inlet(&self) -> Inlet<Self::In> {
        self.inlet.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<In> Stage for PatchReplicas<In>
where
    In: AppData + TargetReplicas,
{
    #[inline]
    fn name(&self) -> SharedString {
        SharedString::Borrowed(STAGE_NAME)
    }

    #[tracing::instrument(level="info", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.do_check().await.map_err(|err| ProctorError::PhaseError(err.into()))?;
        Ok(())
    }

    #[tracing::instrument(level="info", name="run patch replicas", skip(self))]
    async fn run(&mut self) -> ProctorResult<()> {
        self.do_run().await.map_err(|err| ProctorError::PhaseError(err.into()))?;
        Ok(())
    }

    #[tracing::instrument(level="info", skip(self))]
    async fn close(self: Box<Self>) -> ProctorResult<()> {
        self.do_close().await.map_err(|err| ProctorError::PhaseError(err.into()))?;
        Ok(())
    }
}

impl<In> PatchReplicas<In>
where
    In: AppData + TargetReplicas,
{
    #[inline]
    async fn do_check(&self) -> Result<(), ExecutionPhaseError> {
        self.inlet.check_attachment().await?;
        Ok(())
    }

    #[inline]
    async fn do_run(&mut self) -> Result<(), ExecutionPhaseError> { todo!() }

    #[inline]
    async fn do_close(mut self: Box<Self>) -> Result<(), ExecutionPhaseError> {
        tracing::trace!("closing patch replicas execution phase inlet.");
        self.inlet.close().await;
        Ok(())
    }
}
