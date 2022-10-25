#![forbid(unsafe_code)]
#![warn(
    clippy::cargo,
    clippy::suspicious,
    // missing_docs,
    clippy::nursery,
    rust_2018_idioms
)]

pub mod engine;
pub mod flink;
pub mod kubernetes;
pub mod math;
pub mod metrics;
pub mod model;
pub mod phases;
pub mod settings;

pub type Result<T> = anyhow::Result<T>;

pub type Env<T> = proctor::Env<T>;

pub type CorrelationId = pretty_snowflake::Id<phases::PhaseDataT>;
