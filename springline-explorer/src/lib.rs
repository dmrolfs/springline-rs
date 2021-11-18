pub mod app_menu;
pub mod policy;
pub mod settings;

pub use app_menu::AppMenu;
use console::style;
use dialoguer::console::Style;
use dialoguer::theme::ColorfulTheme;
use once_cell::sync::Lazy;
use settings_loader::SettingsLoader;
use springline::settings::{CliOptions, Settings};

pub type Result<T> = anyhow::Result<T>;

pub type MenuAction<S> = Box<dyn Fn(&mut S) -> anyhow::Result<()> + Send + Sync>;

static THEME: Lazy<ColorfulTheme> = Lazy::new(|| ColorfulTheme {
    values_style: Style::new().yellow().dim(),
    ..ColorfulTheme::default()
});

#[derive(Debug, Clone, PartialEq)]
pub struct ExplorerState {
    pub options: CliOptions,
    pub settings: Settings,
}

impl ExplorerState {
    pub fn new(options: CliOptions) -> Result<Self> {
        let settings = Self::load_settings(&options)?;
        Ok(Self { options, settings })
    }

    pub fn load_settings(options: &CliOptions) -> Result<Settings> {
        eprintln!("\n{}\n", style(format!("DMR: OPTIONS: {:#?}", options)).bright().blue());
        let settings = Settings::load(options)?;
        Ok(settings)
    }
}
