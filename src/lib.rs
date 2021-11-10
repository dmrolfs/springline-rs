extern crate enum_display_derive;

pub mod engine;
pub mod metrics;
pub mod phases;
pub mod settings;
pub mod bin;

pub type Result<T> = anyhow::Result<T>;
