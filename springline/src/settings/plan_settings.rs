use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::phases::plan::{PerformanceRepositorySettings, SpikeSettings};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(PartialEq))]
pub struct PlanSettings {
    pub min_scaling_step: u32,
    pub restart: Duration,
    pub max_catch_up: Duration,
    pub recovery_valid: Duration,
    pub performance_repository: PerformanceRepositorySettings,
    pub window: usize,
    pub spike: SpikeSettings,
}

impl Default for PlanSettings {
    fn default() -> Self {
        Self {
            min_scaling_step: 1,
            restart: Duration::from_secs(2 * 60),
            max_catch_up: Duration::from_secs(13 * 60),
            recovery_valid: Duration::from_secs(5 * 60),
            performance_repository: PerformanceRepositorySettings::default(),
            window: 20,
            spike: SpikeSettings::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_test::{assert_tokens, Token};

    use super::*;
    use crate::phases::plan::PerformanceRepositoryType;

    #[test]
    fn test_serde_plan_settings() {
        let settings = PlanSettings {
            min_scaling_step: 2,
            restart: Duration::from_secs(3 * 60),
            max_catch_up: Duration::from_secs(10 * 60),
            recovery_valid: Duration::from_secs(5 * 60),
            performance_repository: PerformanceRepositorySettings {
                storage: PerformanceRepositoryType::File,
                storage_path: Some("./resources/foo.data".to_string()),
            },
            window: 20,
            spike: SpikeSettings {
                std_deviation_threshold: 3.1,
                influence: 0.75,
                length_threshold: 3,
            },
        };

        assert_tokens(
            &settings,
            &vec![
                Token::Struct { name: "PlanSettings", len: 7 },
                Token::Str("min_scaling_step"),
                Token::U32(2),
                Token::Str("restart"),
                Token::Struct { name: "Duration", len: 2 },
                Token::Str("secs"),
                Token::U64(3 * 60),
                Token::Str("nanos"),
                Token::U32(0),
                Token::StructEnd,
                Token::Str("max_catch_up"),
                Token::Struct { name: "Duration", len: 2 },
                Token::Str("secs"),
                Token::U64(10 * 60),
                Token::Str("nanos"),
                Token::U32(0),
                Token::StructEnd,
                Token::Str("recovery_valid"),
                Token::Struct { name: "Duration", len: 2 },
                Token::Str("secs"),
                Token::U64(5 * 60),
                Token::Str("nanos"),
                Token::U32(0),
                Token::StructEnd,
                Token::Str("performance_repository"),
                Token::Struct { name: "PerformanceRepositorySettings", len: 2 },
                Token::Str("storage"),
                Token::UnitVariant { name: "PerformanceRepositoryType", variant: "file" },
                Token::Str("storage_path"),
                Token::Some,
                Token::Str("./resources/foo.data"),
                Token::StructEnd,
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
