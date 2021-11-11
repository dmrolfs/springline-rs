pub mod app_menu;
pub mod policy_explorer;

pub use app_menu::AppMenu;
use dialoguer::console::Style;
use dialoguer::theme::ColorfulTheme;
use once_cell::sync::Lazy;


pub type MenuAction = Box<dyn Fn() -> anyhow::Result<()> + Send + Sync>;

static THEME: Lazy<ColorfulTheme> = Lazy::new(|| ColorfulTheme {
    values_style: Style::new().yellow().dim(),
    ..ColorfulTheme::default()
});

