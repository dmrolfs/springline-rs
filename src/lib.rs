#[macro_use]
extern crate enum_display_derive;

pub mod error;
pub mod phases;
pub mod settings;

pub type Result<T> = anyhow::Result<T>;
