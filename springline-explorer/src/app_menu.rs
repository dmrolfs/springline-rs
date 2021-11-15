use dialoguer::console::style;
use dialoguer::FuzzySelect;
use once_cell::sync::Lazy;
use settings_loader::SettingsLoader;
use springline::settings::{CliOptions, Settings};

use crate::{ExplorerState, MenuAction, Result, THEME};

static SELECTION_ACTIONS: Lazy<[(&str, MenuAction); 3]> = Lazy::new(|| {
    [
        ("Settings", Box::new(AppMenu::establish_settings)),
        ("Eligibility", Box::new(AppMenu::explore_eligibility_policy)),
        ("exit", Box::new(AppMenu::exit_action)),
    ]
});

static SELECTIONS: Lazy<Vec<&str>> = Lazy::new(|| SELECTION_ACTIONS.iter().map(|s| s.0).collect());

#[derive(Debug)]
pub struct AppMenu {
    state: ExplorerState,
}

impl AppMenu {
    pub fn new(options: CliOptions) -> Result<Self> {
        let state = ExplorerState::new(options)?;
        Ok(Self { state })
    }

    pub fn interact(&mut self) -> anyhow::Result<()> {
        loop {
            let selected = FuzzySelect::with_theme(&*THEME)
                .with_prompt("\nWhat do you want to do next?")
                .default(0)
                .items(SELECTIONS.as_slice())
                .interact()
                .expect("failed to select main menu action");

            // match selected.and_then(|pos| SELECTION_ACTIONS.get(pos)) {
            match SELECTION_ACTIONS.get(selected) {
                Some((label, action)) => {
                    if let Err(err) = action(&mut self.state) {
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

    pub fn establish_settings(state: &mut ExplorerState) -> anyhow::Result<()> {
        todo!()
    }

    pub fn explore_eligibility_policy(state: &mut ExplorerState) -> anyhow::Result<()> {
        todo!()
    }

    pub fn exit_action(_state: &mut ExplorerState) -> anyhow::Result<()> {
        eprintln!(
            "\n{} {}",
            style("Exiting").bold(),
            style("Springline Policy Explorer").green().bold()
        );
        std::process::exit(0)
    }
}
