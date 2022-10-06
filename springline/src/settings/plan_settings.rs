use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use crate::phases::plan::{
    BenchmarkRange, ClippingHandlingSettings, PerformanceRepositorySettings, ScaleDirection,
    SpikeSettings,
};

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(PartialEq))]
pub struct PlanSettings {
    /// Minimum cluster size used in rescale planning.
    pub min_cluster_size: u32,

    /// Minimum scaling step used in rescale planing.
    pub min_scaling_step: u32,

    /// Starting estimated time used to restart the flink job. This time is measured and fed back
    /// by springline for subsequent rescale planning.
    #[serde_as(as = "HashMap<serde_with::DisplayFromStr, serde_with::DurationSeconds<u64>>")]
    #[serde(rename = "direction_restart_secs")]
    pub direction_restart: HashMap<ScaleDirection, Duration>,

    /// Time allowed for the cluster to catch up processing messages after rescaling restart.
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(rename = "max_catch_up_secs")]
    pub max_catch_up: Duration,

    /// Duration after restart and catch up where the job is considered to be running.
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(rename = "recovery_valid_secs")]
    pub recovery_valid: Duration,

    /// Optional setting that directs how springline should handle detected telemetry clipping,
    /// during which necessary telemetry is not provided by Flink possibly due to misconfiguration or
    /// instability. For misconfiguration, if the nonsource parallelism is allowed to get too high,
    /// the job operators can be "overwhelmed" by the effort to aggregate data that source telemetry is
    /// not published by Flink. The default is to `ignore`, and the corresponding decision rules may not
    /// be able to recognize the need to rescale. `permanment_limit` will identify the parallelism level
    /// that clipping happens so that planning can take that into account to cap the parallelism just
    /// below it. `temporary_limit` allows you to set a timeout when the clipping level is reset.
    #[serde(default = "ClippingHandlingSettings::default")]
    pub clipping_handling: ClippingHandlingSettings,

    pub performance_repository: PerformanceRepositorySettings,

    /// Optional population of node : workload rates used to estimate best cluster size for projected
    /// workload. The performance history of the job is created based on the actual measured
    /// performance of the job, so will change for different resourcing, configurations and even as it
    /// runs. The Job's history is saved in the `performance_repository`. These default settings to tune
    /// initial rescale calculations, which without initial values tend to vary wildly, but converge
    /// over time while springline maps performance.
    ///
    /// optional initial map of workloads at which springline will rescale upward.
    /// For ech given `job_parallelism` specify the `hi_rate` (for rescale up) and/or the `lo_rate` (for
    /// rescale down) default benchmark workload rates. The table can be sparse, and springline
    /// calibrates its calculations accordingly.
    #[serde(default = "Default::default", skip_serializing_if = "Vec::is_empty")]
    pub benchmarks: Vec<BenchmarkRange>,

    pub window: usize,
    pub spike: SpikeSettings,
}

impl Default for PlanSettings {
    fn default() -> Self {
        let direction_restart = maplit::hashmap! {
            ScaleDirection::Up => Duration::from_secs(210),
            ScaleDirection::Down => Duration::from_secs(600),
        };

        Self {
            min_cluster_size: 1,
            min_scaling_step: 1,
            direction_restart,
            max_catch_up: Duration::from_secs(13 * 60),
            recovery_valid: Duration::from_secs(5 * 60),
            clipping_handling: ClippingHandlingSettings::Ignore,
            performance_repository: PerformanceRepositorySettings::default(),
            benchmarks: Vec::default(),
            window: 20,
            spike: SpikeSettings::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::flink::Parallelism;
    use serde_test::{assert_tokens, Token};

    use super::*;
    use crate::phases::plan::PerformanceRepositoryType;

    #[test]
    fn test_serde_plan_settings() {
        let settings = PlanSettings {
            min_cluster_size: 3,
            min_scaling_step: 2,
            direction_restart: maplit::hashmap! { ScaleDirection::Up => Duration::from_secs(210), },
            max_catch_up: Duration::from_secs(10 * 60),
            clipping_handling: ClippingHandlingSettings::TemporaryLimit {
                reset_timeout: Duration::from_secs(350),
            },
            performance_repository: PerformanceRepositorySettings {
                storage: PerformanceRepositoryType::File,
                storage_path: Some("./resources/foo.data".to_string()),
            },
            benchmarks: vec![
                BenchmarkRange::new(Parallelism::new(1), None, Some(3.14159.into())),
                BenchmarkRange::new(Parallelism::new(27), None, Some(79.3875.into())),
            ],
            spike: SpikeSettings {
                std_deviation_threshold: 3.1,
                influence: 0.75,
                length_threshold: 3,
            },
            ..PlanSettings::default()
        };

        assert_tokens(
            &settings,
            &vec![
                Token::Struct { name: "PlanSettings", len: 10 },
                Token::Str("min_cluster_size"),
                Token::U32(3),
                Token::Str("min_scaling_step"),
                Token::U32(2),
                Token::Str("direction_restart_secs"),
                Token::Map { len: Some(1) },
                Token::Str("up"),
                Token::U64(210),
                Token::MapEnd,
                Token::Str("max_catch_up_secs"),
                Token::U64(10 * 60),
                Token::Str("recovery_valid_secs"),
                Token::U64(5 * 60),
                Token::Str("clipping_handling"),
                Token::StructVariant {
                    name: "ClippingHandlingSettings",
                    variant: "temporary_limit",
                    len: 1,
                },
                Token::Str("reset_timeout_secs"),
                Token::U64(350),
                Token::StructVariantEnd,
                Token::Str("performance_repository"),
                Token::Struct { name: "PerformanceRepositorySettings", len: 2 },
                Token::Str("storage"),
                Token::UnitVariant { name: "PerformanceRepositoryType", variant: "file" },
                Token::Str("storage_path"),
                Token::Some,
                Token::Str("./resources/foo.data"),
                Token::StructEnd,
                Token::Str("benchmarks"),
                Token::Seq { len: Some(2) },
                Token::Struct { name: "BenchmarkRange", len: 2 },
                Token::Str("job_parallelism"),
                Token::U32(1),
                Token::Str("hi_rate"),
                Token::Some,
                Token::NewtypeStruct { name: "RecordsPerSecond" },
                Token::F64(3.14159),
                Token::StructEnd,
                Token::Struct { name: "BenchmarkRange", len: 2 },
                Token::Str("job_parallelism"),
                Token::U32(27),
                Token::Str("hi_rate"),
                Token::Some,
                Token::NewtypeStruct { name: "RecordsPerSecond" },
                Token::F64(79.3875),
                Token::StructEnd,
                Token::SeqEnd,
                Token::Str("window"),
                Token::U64(20),
                Token::Str("spike"),
                Token::Struct { name: "SpikeSettings", len: 3 },
                Token::Str("std_deviation_threshold"),
                Token::F64(3.1),
                Token::Str("influence"),
                Token::F64(0.75),
                Token::Str("length_threshold"),
                Token::U64(3),
                Token::StructEnd,
                Token::StructEnd,
            ],
        )
    }
}
