pub mod context;
pub mod policy;
pub mod result;

use crate::phases::plan::PlanningOutcome;
use crate::settings::Settings;
pub use context::*;
pub use policy::*;
use proctor::graph::stage::ThroughStage;
pub use result::*;

pub type GoveranceOutcome = PlanningOutcome;

#[tracing::instrument(level = "info", skip(_settings))]
pub async fn make_governance_phase(
    _settings: &Settings,
) -> anyhow::Result<Box<dyn ThroughStage<PlanningOutcome, GoveranceOutcome>>> {
    todo!()
}
