extern crate enum_display_derive;

// #[macro_use]
// extern crate fix_hidden_lifetime_bug;

pub mod engine;
pub mod phases;
pub mod settings;

pub type Result<T> = anyhow::Result<T>;
