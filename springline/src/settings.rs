pub mod collection_settings;
pub mod engine_settings;
pub mod execution_settings;
pub mod plan_settings;

pub use collection_settings::*;
pub use engine_settings::*;
pub use execution_settings::*;
pub use plan_settings::*;

use crate::phases::decision::DecisionTemplateData;
use crate::phases::eligibility::policy::EligibilityTemplateData;
use crate::phases::governance::GovernanceTemplateData;
use clap::Parser;
use config::builder::DefaultState;
use config::ConfigBuilder;
use proctor::elements::PolicySettings;
use serde::{Deserialize, Serialize};
use settings_loader::common::http::HttpServerSettings;
use settings_loader::{LoadingOptions, SettingsError, SettingsLoader};
use std::path::PathBuf;

pub type EligibilitySettings = PolicySettings<EligibilityTemplateData>;
pub type DecisionSettings = PolicySettings<DecisionTemplateData>;
pub type GovernanceSettings = PolicySettings<GovernanceTemplateData>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Settings {
    pub http: HttpServerSettings,
    #[serde(default)]
    pub engine: EngineSettings,
    #[serde(default)]
    pub collection: CollectionSettings,
    #[serde(default)]
    pub eligibility: EligibilitySettings,
    #[serde(default)]
    pub decision: DecisionSettings,
    #[serde(default)]
    pub plan: PlanSettings,
    #[serde(default)]
    pub governance: GovernanceSettings,
    pub execution: ExecutionSettings,
}

impl SettingsLoader for Settings {
    type Options = CliOptions;
}

#[derive(Parser, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[clap(version = "0.1.0", author = "Damon Rolfs")]
pub struct CliOptions {
    /// override environment-based configuration file to load.
    /// Default behavior is to load configuration based on `APP_ENVIRONMENT` envvar.
    #[clap(short, long)]
    config: Option<PathBuf>,

    /// specify path to secrets configuration file
    #[clap(short, long)]
    secrets: Option<PathBuf>,

    /// Override default location from which to load configuration files. Default directory is
    /// ./resources.
    resources: Option<PathBuf>,

    /// Specify the machine id [0, 31) used in correlation id generation, overriding what may be set
    /// in an environment variable. This id should be unique for the entity type within a cluster
    /// environment. Different entity types can use the same machine id.
    machine_id: Option<i8>,

    /// Specify the node id [0, 31) used in correlation id generation, overriding what may be set
    /// in an environment variable. This id should be unique for the entity type within a cluster
    /// environment. Different entity types can use the same machine id.
    node_id: Option<i8>,
}

impl LoadingOptions for CliOptions {
    type Error = SettingsError;

    fn config_path(&self) -> Option<PathBuf> {
        self.config.clone()
    }

    fn resources_path(&self) -> Option<PathBuf> {
        self.resources.clone()
    }

    fn secrets_path(&self) -> Option<PathBuf> {
        self.secrets.clone()
    }

    fn load_overrides(&self, config: ConfigBuilder<DefaultState>) -> Result<ConfigBuilder<DefaultState>, Self::Error> {
        let config = match self.machine_id {
            None => config,
            Some(machine_id) => config.set_override("machine_id", machine_id as i64)?,
        };

        let config = match self.node_id {
            None => config,
            Some(node_id) => config.set_override("node_id", node_id as i64)?,
        };

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::phases::plan::{PerformanceRepositorySettings, PerformanceRepositoryType, SpikeSettings};
    use claim::assert_ok;
    use config::{Config, FileFormat};
    use lazy_static::lazy_static;
    use pretty_assertions::assert_eq;
    use proctor::elements::PolicySource;
    use proctor::phases::collection::SourceSetting;
    use std::collections::HashMap;
    use std::env::VarError;
    use std::panic::{RefUnwindSafe, UnwindSafe};
    use std::sync::Mutex;
    use std::time::Duration;
    use std::{env, panic};

    lazy_static! {
        static ref SERIAL_TEST: Mutex<()> = Default::default();
    }

    /// Sets environment variables to the given value for the duration of the closure.
    /// Restores the previous values when the closure completes or panics, before unwinding the panic.
    pub fn with_env_vars<F>(label: &str, kvs: Vec<(&str, Option<&str>)>, closure: F)
    where
        F: Fn() + UnwindSafe + RefUnwindSafe,
    {
        let guard = SERIAL_TEST.lock().unwrap();
        let mut old_kvs: Vec<(&str, Result<String, VarError>)> = Vec::new();
        for (k, v) in kvs {
            let old_v = env::var(k);
            old_kvs.push((k, old_v));
            match v {
                None => env::remove_var(k),
                Some(v) => env::set_var(k, v),
            }
        }
        eprintln!("W_ENV[{}]: OLD_KVS: {:?}", label, old_kvs);
        let old_kvs_2 = old_kvs.clone();

        match panic::catch_unwind(|| {
            closure();
        }) {
            Ok(_) => {
                eprintln!("W_END[{}]: OK - resetting env to: {:?}", label, old_kvs);
                for (k, v) in old_kvs {
                    reset_env(k, v);
                }
            }
            Err(err) => {
                eprintln!("W_END[{}]: Err - resetting env to: {:?}", label, old_kvs);
                for (k, v) in old_kvs {
                    reset_env(k, v);
                }
                drop(guard);
                panic::resume_unwind(err);
            }
        };
        for (k, v) in old_kvs_2 {
            eprintln!(
                "W_END[{}] RESET ACTUAL: {:?}:{:?} expected:{:?}",
                label,
                k,
                env::var(k),
                v
            );
        }
    }

    fn reset_env(k: &str, old: Result<String, VarError>) {
        if let Ok(v) = old {
            env::set_var(k, v);
        } else {
            env::remove_var(k);
        }
    }

    #[test]
    fn test_basic_load() {
        let c = assert_ok!(config::Config::builder()
            .add_source(config::File::from(std::path::PathBuf::from("./tests/data/settings.ron")))
            // .add_source(config::File::from(std::path::PathBuf::from("./resources/application.ron")))
            .build());

        let expected = Settings {
            http: HttpServerSettings { host: "0.0.0.0".to_string(), port: 8000 },
            engine: Default::default(),
            collection: CollectionSettings {
                sources: maplit::hashmap! {
                    "foo".to_string() => SourceSetting::Csv { path: PathBuf::from("../resources/bar.toml"), },
                },
            },
            eligibility: EligibilitySettings {
                policies: vec![
                    assert_ok!(PolicySource::from_template_file("../resources/eligibility.polar")),
                    assert_ok!(PolicySource::from_template_string(
                        "eligibility_basis",
                        r##"|
    |                        eligible(_, _context, length) if length = 13;
    |                        eligible(_item, context, c) if
    |                            c = context.custom() and
    |                            c.cat = "Otis" and
    |                            cut;
    |                    "##,
                    )),
                ],
                ..EligibilitySettings::default()
            },
            decision: DecisionSettings {
                policies: vec![
                    assert_ok!(PolicySource::from_complete_file("../resources/decision.polar")),
                    assert_ok!(PolicySource::from_complete_file("../resources/decision_basis.polar")),
                ],
                template_data: Some(DecisionTemplateData {
                    basis: "decision_basis".to_string(),
                    max_healthy_lag: Some(133.),
                    min_healthy_lag: 0.,
                    max_healthy_cpu_load: Some(0.7),
                    ..DecisionTemplateData::default()
                }),
                ..DecisionSettings::default()
            },
            plan: PlanSettings {
                min_scaling_step: 2,
                restart: Duration::from_secs(120),
                max_catch_up: Duration::from_secs(600),
                recovery_valid: Duration::from_secs(300),
                performance_repository: PerformanceRepositorySettings {
                    storage: PerformanceRepositoryType::File,
                    storage_path: Some("./tests/data/performance.data".to_string()),
                },
                window: 20,
                spike: SpikeSettings {
                    std_deviation_threshold: 5.0,
                    influence: 0.75,
                    length_threshold: 3,
                },
            },
            governance: GovernanceSettings {
                policies: vec![assert_ok!(PolicySource::from_complete_file(
                    "../resources/governance.polar"
                ))],
                ..GovernanceSettings::default()
            },
            execution: ExecutionSettings,
        };

        // let exp_rep = assert_ok!(ron::to_string(&expected));
        // assert_eq!(exp_rep.as_str(), "");

        let actual: Settings = assert_ok!(c.try_into());

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_load_eligibility_settings() -> anyhow::Result<()> {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_load_eligibility_settings");
        let _ = main_span.enter();

        let config = assert_ok!(Config::builder()
            .add_source(config::File::from_str(
                r###"
                (
                    required_subscription_fields: [],
                    optional_subscription_fields: [],
                    policies: [
                        (source: "file", policy: (path:"./resources/eligibility.polar")),
                    ],
                )
                "###,
                FileFormat::Ron
            ))
            .build());

        tracing::info!(?config, "eligibility config loaded.");

        let actual: EligibilitySettings = assert_ok!(config.try_into());
        let expected = EligibilitySettings::default()
            .with_source(PolicySource::from_complete_file("./resources/eligibility.polar")?);
        assert_eq!(actual, expected);
        Ok(())
    }

    #[test]
    fn test_settings_applications_load() -> anyhow::Result<()> {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_settings_applications_load");
        let _ = main_span.enter();

        let options = CliOptions {
            resources: Some("../resources".into()),
            ..CliOptions::default()
        };
        let before_env = Settings::load(&options);
        tracing::info!("from Settings::load: {:?}", before_env);
        let before_env = assert_ok!(before_env);
        assert_eq!(before_env.engine, EngineSettings { machine_id: 1, node_id: 1 });

        with_env_vars(
            "test_settings_applications_load",
            vec![
                ("APP_ENVIRONMENT", None),
                ("APP__ENGINE__MACHINE_ID", Some("7")),
                ("APP__ENGINE__NODE_ID", Some("3")),
            ],
            || {
                let actual: Settings = assert_ok!(Settings::load(&options));
                assert_eq!(actual.engine, EngineSettings { machine_id: 7, node_id: 3 });

                let expected = Settings {
                    http: HttpServerSettings { host: "0.0.0.0".to_string(), port: 8000 },
                    engine: EngineSettings { machine_id: 7, node_id: 3 },
                    collection: CollectionSettings {
                        sources: maplit::hashmap! {
                            "foo".to_string() => SourceSetting::Csv { path: PathBuf::from("./resources/bar.toml"),},
                        },
                    },
                    eligibility: EligibilitySettings::default()
                        .with_source(assert_ok!(PolicySource::from_template_file(
                            "./resources/eligibility.polar"
                        )))
                        .with_source(assert_ok!(PolicySource::from_template_file(
                            "./resources/eligibility_basis.polar"
                        )))
                        .with_template_data(EligibilityTemplateData {
                            basis: "eligibility_basis".to_string(),
                            cooling_secs: Some(15 * 60),
                            stable_secs: Some(15 * 60),
                            custom: HashMap::default(),
                        }),
                    decision: DecisionSettings::default()
                        .with_source(assert_ok!(PolicySource::from_template_file(
                            "./resources/decision.polar"
                        )))
                        .with_source(assert_ok!(PolicySource::from_template_file(
                            "./resources/decision_basis.polar"
                        )))
                        .with_template_data(DecisionTemplateData {
                            basis: "decision_basis".to_string(),
                            max_healthy_lag: Some(133_f64),
                            min_healthy_lag: 0_f64,
                            max_healthy_cpu_load: Some(0.7),
                            max_healthy_network_io: None,
                            custom: HashMap::default(),
                        }),
                    plan: PlanSettings {
                        min_scaling_step: 2,
                        restart: Duration::from_secs(2 * 60),
                        max_catch_up: Duration::from_secs(10 * 60),
                        recovery_valid: Duration::from_secs(5 * 60),
                        performance_repository: PerformanceRepositorySettings {
                            storage: PerformanceRepositoryType::File,
                            storage_path: Some("./tests/data/performance.data".to_string()),
                        },
                        window: 20,
                        spike: SpikeSettings {
                            std_deviation_threshold: 5.,
                            influence: 0.75,
                            length_threshold: 3,
                        },
                    },
                    governance: GovernanceSettings::default().with_source(assert_ok!(
                        PolicySource::from_complete_file("./resources/governance.polar")
                    )),
                    execution: ExecutionSettings,
                };

                assert_eq!(actual, expected);
            },
        );

        Ok(())
    }

    #[ignore]
    #[test]
    fn test_settings_specific_load() -> anyhow::Result<()> {
        Ok(())
    }
}
