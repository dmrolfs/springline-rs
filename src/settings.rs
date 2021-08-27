mod sources;
pub mod plan_settings;
pub use plan_settings::*;

use clap::Clap;
use config::Config;
use proctor::elements::PolicySettings;
use proctor::error::SettingsError;
use proctor::phases::collection::SourceSetting;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::fmt::Display;
use std::path::PathBuf;
use enum_display_derive;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Settings {
    pub collection_sources: HashMap<String, SourceSetting>,
    pub eligibility_policy: PolicySettings,
    pub decision_policy: PolicySettings,
    pub plan: PlanSettings,
}

#[allow(dead_code)]
const ENV_APP_ENVIRONMENT: &'static str = "APP_ENVIRONMENT";
#[allow(dead_code)]
const CONFIGURATION_DIR: &'static str = "configuration";
#[allow(dead_code)]
const APP_CONFIG: &'static str = "application";

impl Settings {
    #[tracing::instrument(level = "info")]
    pub fn load(options: CliOptions) -> Result<Settings, SettingsError> {
        let root_path = std::env::current_dir()?;
        let configuration_path = root_path.join(CONFIGURATION_DIR);
        let mut config = config::Config::default();
        config.merge(config::File::from(configuration_path.join(APP_CONFIG)).required(true))?;
        config = Self::load_configuration(config, &configuration_path, &options)?;
        config = Self::load_secrets(config, &options)?;
        config = Self::load_environment(config, &options)?;

        let settings = config.try_into()?;
        Ok(settings)
    }

    #[tracing::instrument(level = "info", skip(config, options))]
    fn load_configuration(
        mut config: Config,
        configuration_path: &PathBuf,
        options: &CliOptions,
    ) -> Result<Config, SettingsError> {
        match &options.config {
            Some(config_path) => {
                let config_path = PathBuf::from(config_path);
                tracing::info!("looking for {} config at: {:?}", APP_CONFIG, config_path);
                config.merge(config::File::from(config_path).required(true))?;
            }

            None => {
                let environment = std::env::var(ENV_APP_ENVIRONMENT)?
                    .try_into()
                    .unwrap_or_else(|_| Environment::Local);

                tracing::info!(
                    "looking for {} config at: {:?}",
                    environment,
                    configuration_path
                );
                config.merge(
                    config::File::from(configuration_path.join(environment.as_ref()))
                        .required(true),
                )?;
            }
        }

        Ok(config)
    }

    #[tracing::instrument(level = "info", skip(config, options))]
    fn load_secrets(mut config: Config, options: &CliOptions) -> Result<Config, SettingsError> {
        if let Some(ref secrets_path) = options.secrets {
            let secrets_path = PathBuf::from(secrets_path);
            tracing::info!("looking for secrets at: {:?}", secrets_path);
            config.merge(config::File::from(secrets_path).required(true))?;
        }

        Ok(config)
    }

    #[tracing::instrument(level = "info", skip(config, _options))]
    fn load_environment(
        mut config: Config,
        _options: &CliOptions,
    ) -> Result<Config, SettingsError> {
        let config_env = config::Environment::with_prefix("app").separator("__");
        tracing::info!(
            "loading environment properties with prefix: {:?}",
            config_env
        );
        config.merge(config_env)?;
        Ok(config)
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
