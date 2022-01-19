#![forbid(unsafe_code)]
#![warn(
    clippy::cargo,
    // missing_docs,
    clippy::nursery,
    clippy::pedantic,
    future_incompatible,
    rust_2018_idioms
)]

#[macro_use]
extern crate enum_display_derive;

pub mod engine;
pub mod kubernetes;
pub mod metrics;
pub mod phases;
pub mod settings;

pub type Result<T> = anyhow::Result<T>;
