use std::fmt::Debug;
use std::io::Read;
use std::marker::PhantomData;

use chrono::Utc;
use console::StyledObject;
use dialoguer::console::style;
use dialoguer::FuzzySelect;
use oso::Oso;
use proctor::elements::QueryPolicy;

use crate::{ExplorerState, MenuAction, Result, THEME};

mod make_policy;
mod populate;

pub use make_policy::*;
use populate::*;

#[derive(Debug)]
struct State<'a, P>
where
    P: QueryPolicy,
{
    pub phase: String,
    pub data: Option<P::Item>,
    pub context: Option<P::Context>,
    pub app_state: &'a ExplorerState,
}

#[derive(Debug)]
pub struct ExplorePolicy<P> {
    phase: String,
    marker: PhantomData<P>,
}

impl<P> ExplorePolicy<P>
where
    P: QueryPolicy + MakePolicy + 'static,
    <P as QueryPolicy>::Context: PopulateContext + Debug,
    <P as QueryPolicy>::Item: PopulateData + Debug,
{
    pub fn new(phase: impl Into<String>) -> Self {
        Self { phase: phase.into(), marker: PhantomData }
    }

    pub fn interact(&self, state: &mut ExplorerState) -> Result<()> {
        eprintln!(
            "\n\n{} {} {}...\n",
            style("Explore").bold().cyan(),
            style(self.phase.as_str()).bright().cyan(),
            style("Policy").bold().cyan(),
        );

        let mut policy_state = State {
            phase: self.phase.clone(),
            data: None,
            context: None,
            app_state: state,
        };

        loop {
            let set_context = style("Set Context");
            let set_context = if policy_state.context.is_none() {
                set_context.bold().bright()
            } else {
                set_context.green().dim()
            };

            let set_data = style("Set Data");
            let set_data = if policy_state.data.is_none() {
                set_data.bold().bright()
            } else {
                set_data.green().dim()
            };

            let query_policy = style("Query Policy");
            let query_policy = if policy_state.context.is_some() || policy_state.data.is_some() {
                query_policy.bright()
            } else {
                query_policy.bold().dim()
            };

            #[allow(clippy::type_complexity)]
            let menu_actions: [(StyledObject<&str>, Option<MenuAction<State<P>>>); 4] = [
                (set_context.clone(), Some(Box::new(Self::set_context))),
                (set_data.clone(), Some(Box::new(Self::set_data))),
                (query_policy.clone(), Some(Box::new(Self::query_policy))),
                (style("return").dim(), None),
            ];

            let selections: Vec<StyledObject<&str>> = menu_actions.iter().map(|s| s.0.clone()).collect();

            let default_pos = match (policy_state.context.as_ref(), policy_state.data.as_ref()) {
                (None, Some(_)) => 0,
                (Some(_), None) => 1,
                (Some(_), Some(_)) => 2,
                _ => selections.len() - 1,
            };

            let idx = FuzzySelect::with_theme(&*THEME)
                .with_prompt("Prepare data and context for evaluation:")
                .default(default_pos)
                .items(&selections)
                .interact()?;

            match menu_actions.get(idx) {
                Some((label, Some(action))) => {
                    if let Err(err) = action(&mut policy_state) {
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

    fn set_context(state: &mut State<P>) -> Result<()> {
        eprintln!("\nSet Context for query.");
        let context = <P::Context as PopulateContext>::make(Utc::now(), &state.app_state.settings)?;
        eprintln!("\n{}: {:#?}", style("Context").bold(), context);
        state.context = Some(context);
        Ok(())
    }

    fn set_data(state: &mut State<P>) -> Result<()> {
        eprintln!("\nSet Data for query.");
        let data = <P::Item as PopulateData>::make(Utc::now(), &state.app_state.settings)?;
        eprintln!("\n{}: {:#?}", style("Data").bold(), data);
        state.data = Some(data);
        Ok(())
    }

    fn query_policy(state: &mut State<P>) -> Result<()> {
        let data_context = state.data.clone().zip(state.context.clone());
        if data_context.is_none() {
            eprintln!("\n{}", style("Set data and context before policy query.").red());
            return Ok(());
        }
        let (data, context) = data_context.unwrap();

        eprintln!("\nLoading {} policy...", state.phase);
        let policy = <P as MakePolicy>::make(&state.app_state.settings);
        let sources = policy.render_policy_sources()?;
        for s in sources.iter() {
            let mut buffer = String::new();
            let mut f = std::fs::File::open(s.as_ref())?;
            let chars = f.read_to_string(&mut buffer)?;
            eprintln!(
                "\n{source}: {ps:?}\n{spec} [{sz} characters]:\n\n{content}",
                source = style("Source").bold().blue(),
                spec = style("Policy Specification").bold().underlined().blue(),
                ps = s,
                sz = chars,
                content = style(&buffer).bright().blue(),
            );
        }

        let mut oso = Oso::default();
        oso.clear_rules()?;
        oso.load_files(sources)?;
        policy.initialize_policy_engine(&mut oso)?;

        eprintln!("\n{}", style("Policy loaded.").bold().underlined());

        let args = policy.make_query_args(&data, &context);
        let result = policy.query_policy(&oso, args)?;
        eprintln!(
            "\n{} {}",
            style("Policy query").bold(),
            if result.passed {
                style("PASSED").bold().green()
            } else {
                style("FAILED").bold().red()
            }
        );
        eprintln!(
            "{}: {}\n\n",
            style("With bindings").bold(),
            style(format!("{:?}", result.bindings)).bright().blue(),
        );
        Ok(())
    }
}
