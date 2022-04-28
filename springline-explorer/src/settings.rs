use config::builder::DefaultState;
use config::ConfigBuilder;
use dialoguer::console::style;
use dialoguer::{Confirm, Editor, FuzzySelect, Input};
use settings_loader::{environment, LoadingOptions, SettingsLoader};
use springline::settings::Settings;

use crate::{ExplorerState, MenuAction, Result, THEME};

#[derive(Debug)]
pub struct ReviseSettings;

impl ReviseSettings {
    pub fn interact(&self, state: &mut ExplorerState) -> Result<()> {
        eprintln!(
            "\n\n{}...\n  {}: {}\n",
            style("Revise Settings").bold().green(),
            style("Environment").bold().blue(),
            state
                .options
                .environment()
                .map(|e| style(e.to_string()).blue())
                .unwrap_or_else(|| style("not supplied".to_string()).italic().dim())
        );

        let menu_actions: [(&str, Option<MenuAction<ExplorerState>>); 3] = [
            ("Change environment", Some(Box::new(ReviseSettings::change_environment))),
            ("Edit Settings", Some(Box::new(ReviseSettings::edit_settings))),
            ("return", None),
        ];

        let selections: Vec<&str> = menu_actions.iter().map(|s| s.0).collect();

        loop {
            // eprintln!(
            //     "{}: {}",
            //     style("DMR: state:").bold().red(),
            //     style(format!("{:?}", state)).bold().blue()
            // );

            let idx = FuzzySelect::with_theme(&*THEME)
                .with_prompt("What do you want to do with settings?")
                .default(selections.len() - 1)
                .items(&selections)
                .interact()?;

            match menu_actions.get(idx) {
                Some((label, Some(action))) => {
                    if let Err(err) = action(state) {
                        eprintln!("action {} failed: {:?}", style(label).bold().red(), err);
                        return Err(err);
                    }
                },
                Some((_, None)) => {
                    eprintln!("\nreturning...\n");
                    break;
                },
                None => {
                    eprintln!("I don't know how you got here, but your selection is not understood.");
                    break;
                },
            }
        }

        Ok(())
    }

    fn change_environment(state: &mut ExplorerState) -> Result<()> {
        let original = state.options.environment();
        // let environments = Environment::all();
        // let mut selections: Vec<String> = environments.iter().map(|e| e.to_string()).collect();
        // selections.push("none".to_string());

        // let default_pos = state
        //     .options
        //     .environment()
        //     .and_then(|original| environments.iter().position(|e| e == &original))
        //     .unwrap_or(selections.len() - 1);

        let value: String = Input::with_theme(&*THEME)
            .with_prompt("\nEnter the environment you want simulate?")
            .with_initial_text(environment::PRODUCTION.clone())
            .interact_text()?;

        // if idx == selections.len() - 1 {
        //     state.options.environment = None;
        // } else {
        state.options.environment = Some(value.into()); // environments.get(idx).copied();
                                                        // }

        eprintln!(
            "{}",
            style(format!(
                "DMR: original:{:?} =?= updated:{:?} => changed?:{}",
                original,
                state.options.environment,
                state.options.environment != original
            ))
            .bold()
            .red()
        );

        if state.options.environment != original {
            state.settings = ExplorerState::load_settings(&state.options)?;
            eprintln!(
                "{}",
                style(format!(
                    "Explorer settings reloaded using environment: {}.\n",
                    state
                        .options
                        .environment
                        .as_ref()
                        .map(|e| style(e.to_string()).bold().blue())
                        .unwrap_or_else(|| style("none specified".to_string()).dim())
                ))
                .bold()
            );
        }

        Ok(())
    }

    fn edit_settings(state: &mut ExplorerState) -> Result<()> {
        let loaded = Self::load_config_dialog(state)?;
        if Confirm::with_theme(&*THEME)
            .with_prompt("Do you want to review and hand-edit settings?")
            .interact()?
        {
            let settings_rep = serde_json::to_string_pretty(&loaded)?;
            if let Some(edited) = Editor::new().edit(settings_rep.as_str())? {
                eprintln!("{}", style(format!("Edited Settings: {:?}", edited)).bold().red());

                let settings_de: Settings = serde_json::from_str(&edited)?;
                state.settings = settings_de;
            }
        }

        Ok(())
    }

    pub fn load_config_dialog(state: &mut ExplorerState) -> Result<Settings> {
        let mut builder: ConfigBuilder<DefaultState> = config::ConfigBuilder::default();

        eprintln!(
            "\n{}",
            style("First, let's review parts that contribute to settings.").bold(),
        );
        builder = match state.options.config_path() {
            Some(ref config_path) => {
                eprintln!(
                    "\n\tConfiguration set to load directly from: {:?}",
                    style(config_path.as_path()).bold()
                );
                builder.add_source(Settings::make_explicit_config_source(config_path))
            },
            None => {
                let resources = state
                    .options
                    .resources_path()
                    .unwrap_or(std::env::current_dir()?.join(Settings::resources_home()));
                let basename = Settings::app_config_basename();

                eprintln!(
                    "\n\tLooking for: {} file in resources home: {:?}",
                    style(basename).bold(),
                    style(resources.as_path()).bold()
                );
                builder = builder.add_source(Settings::make_implicit_app_config_sources(basename, &resources));

                builder = if let Some(env) = state.options.environment() {
                    eprintln!("\n\tEnvironment complement: {}", style(env.to_string()).bold().blue());
                    builder.add_source(Settings::make_app_environment_source(env, &resources))
                } else {
                    eprintln!("\n\tEnvironment complement: {}", style("none").dim().blue());
                    builder
                };

                builder
            },
        };

        builder = match &state.options.secrets {
            Some(secrets) => {
                eprintln!("\n\tSecrets added from: {:?}", style(secrets.as_path()).bold().red());
                let s = Settings::make_secrets_source(secrets);
                builder.add_source(s)
            },
            None => {
                eprintln!("\n\tNo secrets supplied.");
                builder
            },
        };

        eprintln!(
            "\n\tEnvvars added for prefix:{} and delimiter:{}",
            style(Settings::environment_prefix().to_uppercase()).bold().blue(),
            style(Settings::environment_path_separator().to_uppercase()).bold().blue()
        );
        builder = builder.add_source(Settings::make_environment_variables_source());

        eprintln!(
            "\n\tAdding overrides from CLI options:\n\t{:?}",
            style(&state.options).dim().green()
        );
        eprintln!();
        builder = state.options.load_overrides(builder)?;

        let config = builder.build()?;
        let settings: Settings = config.try_deserialize()?;
        Ok(settings)
    }
}
