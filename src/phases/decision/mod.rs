pub mod context;
pub mod policy;
pub mod result;

use crate::phases::eligibility::EligibilityOutcome;
use crate::phases::MetricCatalog;
use crate::settings::Settings;

pub use context::*;
pub use policy::*;

use proctor::graph::stage::ThroughStage;

pub use result::*;

pub type DecisionOutcome = DecisionResult<MetricCatalog>;

#[tracing::instrument(level = "info", skip(_settings))]
pub async fn make_decision_phase(
    _settings: &Settings,
) -> anyhow::Result<Box<dyn ThroughStage<EligibilityOutcome, DecisionOutcome>>> {
    todo!()
}
