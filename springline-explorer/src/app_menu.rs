use dialoguer::console::style;
use dialoguer::FuzzySelect;
use springline::phases::eligibility::EligibilityPolicy;
use springline::settings::CliOptions;

use crate::policy::ExplorePolicy;
use crate::settings::ReviseSettings;
use crate::{ExplorerState, MenuAction, Result, THEME};

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
        let menu_actions: [(&str, MenuAction<ExplorerState>); 3] = [
            ("Settings", Box::new(AppMenu::explore_settings)),
            ("Eligibility", Box::new(AppMenu::explore_eligibility_policy)),
            ("exit", Box::new(AppMenu::exit_action)),
        ];

        let selections: Vec<&str> = menu_actions.iter().map(|s| s.0).collect();

        loop {
            let idx = FuzzySelect::with_theme(&*THEME)
                .with_prompt("\nWhat do you want to do next?")
                .default(0)
                .items(&selections)
                .interact()
                .expect("failed to select main menu action");

            match menu_actions.get(idx) {
                Some((label, action)) => {
                    if let Err(err) = action(&mut self.state) {
                        eprintln!("action {} failed: {:?}", label, err);
                        break Ok(());
                    }
                },
                None => {
                    eprintln!(
                        "I don't know how you got here, but your selection is not understood."
                    );
                },
            }
        }
    }

    pub fn explore_settings(state: &mut ExplorerState) -> Result<()> {
        ReviseSettings.interact(state)
    }

    pub fn explore_eligibility_policy(state: &mut ExplorerState) -> Result<()> {
        ExplorePolicy::<EligibilityPolicy>::new("eligibility").interact(state)
    }

    pub fn exit_action(_state: &mut ExplorerState) -> Result<()> {
        eprintln!(
            "\n{} {}",
            style("Exiting").bold(),
            style("Springline Policy Explorer").green().bold()
        );
        std::process::exit(0)
    }
}
