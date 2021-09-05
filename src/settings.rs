pub mod collection_settings;
pub mod execution_settings;
pub mod plan_settings;

pub use collection_settings::*;
pub use execution_settings::*;
pub use plan_settings::*;

use crate::error::SettingsError;
use clap::Clap;
use config::builder::DefaultState;
use config::ConfigBuilder;
use proctor::elements::PolicySettings;
use serde::{Deserialize, Serialize};
use std::convert::{TryFrom, TryInto};
use std::fmt::Display;
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

#[allow(dead_code)]
const ENV_APP_ENVIRONMENT: &'static str = "APP_ENVIRONMENT";
#[allow(dead_code)]
const RESOURCES_DIR: &'static str = "resources";
#[allow(dead_code)]
const APP_CONFIG: &'static str = "application";

impl Settings {
    #[tracing::instrument(level = "info")]
    pub fn load(options: CliOptions) -> Result<Settings, SettingsError> {
        let mut config_builder = config::Config::builder();
        config_builder = Self::load_configuration(config_builder, options.config.as_ref())?;
        config_builder = Self::load_secrets(config_builder, &options)?;
        config_builder = Self::load_environment(config_builder, &options)?;
        let config = config_builder.build()?;
        let settings = config.try_into()?;
        Ok(settings)
    }

    #[tracing::instrument(level = "info", skip(config,))]
    fn load_configuration(
        config: ConfigBuilder<DefaultState>,
        specific_config_path: Option<&String>,
    ) -> Result<ConfigBuilder<DefaultState>, SettingsError> {
        match specific_config_path {
            Some(explicit_path) => {
                let path = PathBuf::from(explicit_path);
                tracing::info!("looking for specific config at: {:?}", path);
                let config = config.add_source(config::File::from(path).required(true));
                Ok(config)
            }

            None => {
                let resources_path = std::env::current_dir()?.join(RESOURCES_DIR);
                let config_path = resources_path.join(APP_CONFIG);
                tracing::info!("looking for {} config in: {:?}", APP_CONFIG, resources_path);
                let config = config.add_source(config::File::new(
                    config_path.to_string_lossy().as_ref(),
                    config::FileFormat::Ron,
                ));
                // config.merge(config::File::from(config_path).required(true))?;
                // config.merge(config::File::new(config_path.to_string_lossy().as_ref(), config::FileFormat::Ron))?;

                match std::env::var(ENV_APP_ENVIRONMENT) {
                    Ok(rep) => {
                        let environment: Environment = rep.try_into()?;
                        tracing::info!(
                            "looking for {} config in: {:?}",
                            environment,
                            resources_path
                        );
                        let env_config_path = resources_path.join(environment.as_ref());
                        // config.merge(config::File::from(env_config_path).required(true))?;
                        // config.merge(config::File::with_name(env_config_path.to_string_lossy().as_ref()).required(true))?;
                        let config = config.add_source(
                            config::File::with_name(env_config_path.to_string_lossy().as_ref())
                                .required(true),
                        );
                        Ok(config)
                    }

                    Err(std::env::VarError::NotPresent) => {
                        tracing::warn!(
                            "no environment override on settings specified at env var, {}",
                            ENV_APP_ENVIRONMENT
                        );
                        Ok(config)
                    }

                    Err(err) => Err(err.into()),
                }
            }
        }
    }

    #[tracing::instrument(level = "info", skip(config, options))]
    fn load_secrets(
        config: ConfigBuilder<DefaultState>,
        options: &CliOptions,
    ) -> Result<ConfigBuilder<DefaultState>, SettingsError> {
        let loaded_config = if let Some(ref secrets_path) = options.secrets {
            let secrets_path = PathBuf::from(secrets_path);
            tracing::info!("looking for secrets at: {:?}", secrets_path);
            // config.merge(config::File::from(secrets_path).required(true))?;
            config.add_source(config::File::from(secrets_path).required(true))
        } else {
            config
        };

        Ok(loaded_config)
    }

    #[tracing::instrument(level = "info", skip(config, _options))]
    fn load_environment(
        config: ConfigBuilder<DefaultState>,
        _options: &CliOptions,
    ) -> Result<ConfigBuilder<DefaultState>, SettingsError> {
        let config_env = config::Environment::with_prefix("app").separator("__");
        tracing::info!(
            "loading environment properties with prefix: {:?}",
            config_env
        );
        // config.merge(config_env)?;
        Ok(config.add_source(config_env))
    }
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

#[derive(Debug, Display, PartialEq)]
enum Environment {
    Local,
    Production,
}

impl AsRef<str> for Environment {
    fn as_ref(&self) -> &str {
        match self {
            Environment::Local => "local",
            Environment::Production => "production",
        }
    }
}

impl TryFrom<String> for Environment {
    type Error = SettingsError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "local" => Ok(Self::Local),
            "production" => Ok(Self::Production),
            other => Err(SettingsError::Bootstrap {
                message: format!("{} environment unrecognized", other),
                setting: "environment identification".to_string(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::phases::plan::{
        PerformanceRepositorySettings, PerformanceRepositoryType, SpikeSettings,
    };
    use claim::assert_ok;
    use pretty_assertions::assert_eq;
    use std::time::Duration;
    use proctor::phases::collection::SourceSetting;
    use proctor::elements::PolicySource;
    use config::{Config, FileFormat};

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
        lazy_static::initialize(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_settings_applications_load");
        let _ = main_span.enter();


        let builder = assert_ok!(Settings::load_configuration(Config::builder(), None));
        let c = assert_ok!(builder.build());
        tracing::info!(config=?c, "loaded configuration file");
        let actual: Settings = assert_ok!(c.try_into());

        let expected = Settings {
            collection: CollectionSettings {
                sources: maplit::hashmap! {
                    "foo".to_string() => SourceSetting::Csv { path: PathBuf::from("./resources/bar.toml"),},
                },
            },
            eligibility: PolicySettings::default()
                .with_source(PolicySource::File(PathBuf::from("./resources/eligibility.polar")))
                .with_source(PolicySource::NoPolicy),
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
                .with_source(PolicySource::File(PathBuf::from("./resources/governance_preamble.polar")))
                .with_source(PolicySource::File(PathBuf::from("./resources/governance.polar"))),
            execution: ExecutionSettings,
        };

        assert_eq!(actual, expected);
        Ok(())
    }

    #[ignore]
    #[test]
    fn test_settings_specific_load() -> anyhow::Result<()> {
        Ok(())
    }
}
