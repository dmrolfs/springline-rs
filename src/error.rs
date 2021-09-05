use std::fmt::Debug;
use thiserror::Error;

/// Error variants related to configuration.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SettingsError {
    /// Error working with environment variable
    #[error("{0}")]
    Environment(#[from] std::env::VarError),

    /// Error in configuration settings.
    #[error(transparent)]
    Configuration(#[from] config::ConfigError),

    /// Error in bootstrapping execution from configuration.
    #[error("error during system bootstrap: {message}: {setting}")]
    Bootstrap { message: String, setting: String },

    #[error("{0}")]
    HttpRequestError(#[source] anyhow::Error),

    #[error("{0}")]
    SourceError(#[source] anyhow::Error),

    #[error("{0}")]
    IOError(#[from] std::io::Error),
}

impl From<reqwest::header::InvalidHeaderName> for SettingsError {
    fn from(that: reqwest::header::InvalidHeaderName) -> Self {
        SettingsError::HttpRequestError(that.into())
    }
}

impl From<reqwest::header::InvalidHeaderValue> for SettingsError {
    fn from(that: reqwest::header::InvalidHeaderValue) -> Self {
        SettingsError::HttpRequestError(that.into())
    }
}

// impl From<toml::de::Error> for SettingsError {
//     fn from(that: toml::de::Error) -> Self {
//         SettingsError::SourceError(that.into())
//     }
// }

// impl From<serde_hjson::Error> for SettingsError {
//     fn from(that: serde_hjson::Error) -> Self {
//         SettingsError::SourceError(that.into())
//     }
// }
