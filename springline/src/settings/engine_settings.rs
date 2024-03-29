use std::time::Duration;

use crate::flink::{default_quorum_percentile, DEFAULT_QUORUM_PERCENTILE};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[cfg_attr(test, derive(PartialEq))]
pub struct EngineSettings {
    /// Specify the machine id [0, 31) used in correlation id generation, overriding what may be set
    /// in an environment variable. This id should be unique for the entity type within a cluster
    /// environment. Different entity types can use the same machine id.
    pub machine_id: i32,

    /// Specify the node id [0, 31) used in correlation id generation, overriding what may be set
    /// in an environment variable. This id should be unique for the entity type within a cluster
    /// environment. Different entity types can use the same machine id.
    pub node_id: i32,

    /// Specify the span for telemetry collection into a data window available to eligibility and
    /// decision policies. It's more transparent if this span is set to a duration above what is
    /// used in a policy rule, but it is not required. If a policy rule exceeds this configured
    /// span, then coverage of the window must be more than half, otherwise the condition is not
    /// met. The default is 10 minutes.
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(
        rename = "telemetry_window_secs",
        default = "EngineSettings::default_telemetry_window"
    )]
    pub telemetry_window: Duration,

    /// Optional tuning of the coverage percentage required before metric window thresholds are
    /// met. This parameter enables "sufficient" quorum coverage of a threshold window to be tripped.
    /// This parameter enables policy rules to act on a sufficient telemetry level and enable policy
    /// decisions in the event of flapping telemetry. The default value is 0.8, which means at least
    /// only 80% of the telemetry data within the window needs to exceed the threshold before the
    /// rule is triggered. Values MUST be with (0.0, 1.0], and the default percentile is 0.8.
    #[serde(default = "default_quorum_percentile")]
    pub telemetry_window_quorum_percentile: f64,
}

impl Default for EngineSettings {
    fn default() -> Self {
        Self {
            machine_id: 1,
            node_id: 1,
            telemetry_window: Self::default_telemetry_window(),
            telemetry_window_quorum_percentile: DEFAULT_QUORUM_PERCENTILE,
        }
    }
}

impl EngineSettings {
    pub const fn default_telemetry_window() -> Duration {
        Duration::from_secs(10 * 60)
    }
}
