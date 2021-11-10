use dialoguer::FuzzySelect;
use crate::settings::{CliOptions, Settings};
use settings_loader::SettingsLoader;
use once_cell::sync::Lazy;
use crate::bin::explorer::{MenuAction, THEME};

static SELECTION_ACTIONS: Lazy<[(&str, MenuAction);2]> = Lazy::new(|| {[
    ("Settings", Box::new(AppMenu::establish_settings)),
    ("Eligibility", Box::new(AppMenu::explore_eligibility_policy)),
]});

static SELECTIONS: Lazy<Vec<&str>> = Lazy::new(|| { SELECTION_ACTIONS.iter().map(|s| s.0).collect() });

#[derive(Debug, Clone, PartialEq)]
pub struct AppMenu {
    pub options: CliOptions,
    pub settings: Settings,
}

impl AppMenu {
    pub fn new(options: CliOptions) -> anyhow::Result<Self> {
        let settings = Settings::load(options.clone())?;
        Ok(Self { options, settings, })
    }

    pub fn interact(&mut self) -> anyhow::Result<()> {
        loop {
            let selected = FuzzySelect::with_theme(&*THEME)
                .with_prompt("\nWhat do you want to do next?")
                .default(0)
                .items(SELECTIONS.as_slice())
                .interact()
                .expect("failed to select main menu action");

            match SELECTION_ACTIONS.get(selected) {
                Some((label, action)) => {
                    if let Err(err) = action() {
                        eprintln!("action {} failed: {:?}", label, err);
                        break Ok(());
                    }
                },
                None => {
                    eprintln!("I don't know how you got here, but your selection is not understood.");
                },
            }
        }
    }

    pub fn establish_settings() -> anyhow::Result<()> { todo!() }
    pub fn explore_eligibility_policy() -> anyhow::Result<()> { todo!() }
}