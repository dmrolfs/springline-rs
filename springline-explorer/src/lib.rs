pub mod app_menu;
pub mod policy_explorer;

use std::sync::Mutex;

pub use app_menu::AppMenu;
use dialoguer::console::Style;
use dialoguer::theme::ColorfulTheme;
use once_cell::sync::Lazy;
use pretty_snowflake::{AlphabetCodec, IdPrettifier};
use proctor::ProctorIdGenerator;
use settings_loader::SettingsLoader;
use springline::settings::{CliOptions, Settings};

pub type Result<T> = anyhow::Result<T>;

pub type MenuAction = Box<dyn Fn(&mut ExplorerState) -> anyhow::Result<()> + Send + Sync>;

static THEME: Lazy<ColorfulTheme> = Lazy::new(|| ColorfulTheme {
    values_style: Style::new().yellow().dim(),
    ..ColorfulTheme::default()
});

static ID_GENERATOR: Lazy<Mutex<ProctorIdGenerator<()>>> = Lazy::new(|| Mutex::new(ProctorIdGenerator::default()));

#[derive(Debug, Clone, PartialEq)]
pub struct ExplorerState {
    pub options: CliOptions,
    pub settings: Settings,
}

impl ExplorerState {
    pub fn new(options: CliOptions) -> Result<Self> {
        let settings = Settings::load(&options)?;
        Ok(Self { options, settings })
    }
}
