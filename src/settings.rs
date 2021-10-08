pub mod collection_settings;
pub mod execution_settings;
pub mod plan_settings;

pub use collection_settings::*;
pub use execution_settings::*;
pub use plan_settings::*;

use clap::Clap;
use proctor::elements::PolicySettings;
use serde::{Deserialize, Serialize};
use settings_loader::{LoadingOptions, SettingsError, SettingsLoader};
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Settings {
    pub collection: CollectionSettings,
    pub eligibility: PolicySettings,
    pub decision: PolicySettings,
    pub plan: PlanSettings,
    pub governance: PolicySettings,
    pub execution: ExecutionSettings,
}

impl SettingsLoader for Settings {
    type Options = CliOptions;
}

#[derive(Clap, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[clap(version = "0.1.0", author = "Damon Rolfs")]
pub struct CliOptions {
    /// override environment-based configuration file to load.
    /// Default behavior is to load configuration based on `APP_ENVIRONMENT` envvar.
    #[clap(short, long)]
    config: Option<String>,

    /// specify path to secrets configuration file
    #[clap(short, long)]
    secrets: Option<String>,
}

impl LoadingOptions for CliOptions {
    type Error = SettingsError;

    fn config_path(&self) -> Option<PathBuf> {
        self.config.as_ref().map(PathBuf::from)
    }

    fn secrets_path(&self) -> Option<PathBuf> {
        self.secrets.as_ref().map(PathBuf::from)
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
    fn test_load_eligibility_settings() -> anyhow::Result<()> {
        lazy_static::initialize(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_load_eligibility_settings");
        let _ = main_span.enter();

        let config = assert_ok!(Config::builder()
            .add_source(config::File::from_str(
                r###"
                (
                    required_subscription_fields: [],
                    optional_subscription_fields: [],
                    policies: [
                        (source: "file", policy: "./resources/eligibility.polar"),
                    ],
                )
                "###,
                FileFormat::Ron
            ))
            .build());

        tracing::info!(?config, "eligibility config loaded.");

        let actual: PolicySettings = assert_ok!(config.try_into());
        assert_eq!(
            actual,
            PolicySettings::default().with_source(PolicySource::File(PathBuf::from("./resources/eligibility.polar"))),
        );
        Ok(())
    }

    #[test]
    fn test_settings_applications_load() -> anyhow::Result<()> {
        with_env_vars("test_settings_applications_load", vec![], || {
            lazy_static::initialize(&proctor::tracing::TEST_TRACING);
            let main_span = tracing::info_span!("test_settings_applications_load");
            let _ = main_span.enter();

            // let builder = assert_ok!(Settings::load(Config::builder()));
            // let c = assert_ok!(builder.build());
            // tracing::info!(config=?c, "loaded configuration file");
            let actual: Settings = assert_ok!(Settings::load(CliOptions { config: None, secrets: None }));

            let expected = Settings {
                collection: CollectionSettings {
                    sources: maplit::hashmap! {
                        "foo".to_string() => SourceSetting::Csv { path: PathBuf::from("./resources/bar.toml"),},
                    },
                },
                eligibility: PolicySettings::default()
                    .with_source(PolicySource::File(PathBuf::from("./resources/eligibility.polar")))
                    .with_source(PolicySource::String(
                        r##"
                    eligible(_, _context, length) if length = 13;
                    eligible(_item, context, c) if
                    c = context.custom() and
                    c.cat = "Otis" and
                    cut;
                "##
                        .to_string(),
                    )),
                decision: PolicySettings::default()
                    .with_source(PolicySource::File(PathBuf::from("./resources/decision_preamble.polar")))
                    .with_source(PolicySource::File(PathBuf::from("./resources/decision.polar"))),
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
                governance: PolicySettings::default()
                    .with_source(PolicySource::File(PathBuf::from(
                        "./resources/governance_preamble.polar",
                    )))
                    .with_source(PolicySource::File(PathBuf::from("./resources/governance.polar"))),
                execution: ExecutionSettings,
            };

            assert_eq!(actual, expected);
        });
        Ok(())
    }

    #[ignore]
    #[test]
    fn test_settings_specific_load() -> anyhow::Result<()> {
        Ok(())
    }
}
