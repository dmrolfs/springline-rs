use chrono::{DateTime, Utc};
use clap::Parser;
use config::builder::DefaultState;
use config::ConfigBuilder;
use dialoguer::console::Style;
use dialoguer::theme::ColorfulTheme;
use dialoguer::{Confirm, Editor, FuzzySelect, Input};
use once_cell::sync::{Lazy, OnceCell};
use oso::Oso;
use pretty_snowflake::{AlphabetCodec, Id, IdPrettifier};
use proctor::elements::{PolicySettings, QueryPolicy, Timestamp, ToTelemetry};
use proctor::tracing::{get_subscriber, init_subscriber};
use proctor::IdGenerator;
use ron::ser::{self as ron_ser, PrettyConfig};
use serde::de::DeserializeOwned;
use serde::Serialize;
use settings_loader::{Environment, LoadingOptions, SettingsLoader};
use springline::phases::eligibility::{
    ClusterStatus, EligibilityContext, EligibilityPolicy, EligibilityTemplateData, TaskStatus,
};
use springline::phases::{ClusterMetrics, FlowMetrics, JobHealthMetrics, MetricCatalog};
use springline::settings::{CliOptions, EligibilitySettings, Settings};
use std::collections::HashMap;
use std::io::Read;
use std::process::exit;
use std::str::FromStr;
use std::sync::Mutex;
use std::thread::sleep;
use tailcall::tailcall;
use tracing::instrument::WithSubscriber;
use trim_margin::MarginTrimmable;

static BASE_OPTIONS: OnceCell<CliOptions> = OnceCell::new();
static ID_GENERATOR: Lazy<Mutex<IdGenerator>> =
    Lazy::new(|| Mutex::new(IdGenerator::single_node(IdPrettifier::<AlphabetCodec>::default())));


static THEME: Lazy<ColorfulTheme> = Lazy::new(|| ColorfulTheme {
    values_style: Style::new().yellow().dim(),
    ..ColorfulTheme::default()
});

fn app_menu() {
    let menu_selections: Vec<(&str, Box<dyn Fn() -> anyhow::Result<()>>)> = vec![
        ("View configuration", Box::new(view_config)),
        ("Explore Eligibility Policy", Box::new(explore_eligibility)),
        ("Exit", Box::new(exit_action)),
    ];

    let selections: Vec<&str> = menu_selections.iter().map(|s| s.0).collect();

    loop {
        let selection = FuzzySelect::with_theme(&*THEME)
            .with_prompt("\nWhat do you want to do next?")
            .default(0)
            .items(selections.as_slice())
            .interact()
            .expect("failed to select main menu action");

        match menu_selections.get(selection) {
            Some((label, action)) => match action() {
                Ok(()) => (),
                Err(err) => {
                    eprintln!("action {} failed: {:?}", label, err);
                    break;
                }
            },
            None => {
                eprintln!("I don't know how you got here, but your selection is not understood.");
            }
        }

        eprintln!("STOPPING...");
    }
}

fn exit_action() -> anyhow::Result<()> {
    exit(0)
}

fn view_config() -> anyhow::Result<()> {
    let settings = load_config_dialog()?;
    if Confirm::new()
        .with_prompt("Do you want to see the resolved configuration?")
        .interact()?
    {
        let settings_rep = ron_ser::to_string_pretty(&settings, PrettyConfig::default())?;
        let _edited = Editor::new().edit(settings_rep.as_str())?;
    }
    Ok(())
}

fn explore_eligibility() -> anyhow::Result<()> {
    eprintln!("\nExploring Eligibility Policy...");
    let settings = edit_policy_settings("eligibility", load_config_dialog()?.eligibility)?;
    eprintln!("\n\n resolved: {:#?}", settings);
    let mut policy = EligibilityPolicy::new(&settings);
    let sources = policy.render_policy_sources()?;
    for s in sources.iter() {
        let mut buffer = String::new();
        let mut f = std::fs::File::open(s.as_ref())?;
        let chars = f.read_to_string(&mut buffer)?;
        eprintln!("\n\nsource: {:#?}\n[{}]\n{}", s, chars, buffer);
    }

    let mut oso = Oso::default();
    oso.clear_rules()?;
    oso.load_files(sources)?;
    policy.initialize_policy_engine(&mut oso)?;

    eprintln!("Eligibility policies loaded.");
    eprintln!("\nSet environment context for telemetry evaluation.");

    let now = Utc::now();
    let context = make_eligibility_context(now, &settings)?;
    eprintln!("Context: {:#?}", context);

    let id_gen = &mut ID_GENERATOR.lock().unwrap();
    let mc = MetricCatalog {
        timestamp: now.into(),
        correlation_id: id_gen.next_id(),
        health: JobHealthMetrics::default(),
        flow: FlowMetrics::default(),
        cluster: ClusterMetrics::default(),
        custom: HashMap::default(),
    };

    let telemetry = make_metric_catalog(mc)?;
    eprintln!("submitting telemetry into policy:\n{:#?}", telemetry);

    let args = policy.make_query_args(&telemetry, &context);
    let result = policy.query_policy(&oso, args)?;
    eprintln!("eligibility policy result: {:#?}", result);

    Ok(())
}

fn make_eligibility_context(now: DateTime<Utc>, settings: &EligibilitySettings) -> anyhow::Result<EligibilityContext> {
    let is_deploying = Confirm::with_theme(&*THEME)
        .with_prompt("Is the cluster actively deploying at policy evaluation?")
        .interact()?;

    let since_last_deployment = Input::with_theme(&*THEME)
        .with_prompt("How many seconds before policy evaluation was the last deployment?")
        .default(settings.template_data.clone().and_then(|td| td.cooling_secs).unwrap_or(0))
        .interact()?;
    let last_deployment = now - chrono::Duration::seconds(since_last_deployment as i64);

    let last_failure = if Confirm::with_theme(&*THEME).with_prompt("Model a failure?").interact()? {
        let since_failure = Input::with_theme(&*THEME)
            .with_prompt("How many seconds before policy evaluation did the failure occur?")
            .default(settings.template_data.clone().and_then(|td| td.stable_secs).unwrap_or(0))
            .interact()?;
        Some(now - chrono::Duration::seconds(since_failure as i64))
    } else {
        None
    };

    let mut custom = HashMap::default();
    loop {
        let key: String = Input::with_theme(&*THEME)
            .with_prompt("Add custom property?")
            .default("".to_string())
            .allow_empty(true)
            .interact_text()?;

        if key.is_empty() {
            break;
        }

        let value: String = Input::with_theme(&*THEME)
            .with_prompt(format!("{} = ", key))
            .interact_text()?;

        custom.insert(key, value.to_telemetry());
    }

    let id_gen = &mut ID_GENERATOR.lock().unwrap();
    Ok(EligibilityContext {
        correlation_id: id_gen.next_id(),
        timestamp: now.into(),
        task_status: TaskStatus { last_failure },
        cluster_status: ClusterStatus { is_deploying, last_deployment },
        custom,
    })
}

trait Lens {
    type T;
    fn get(&self, telemetry: &Self::T) -> String;
    fn set(&self, telemetry: &mut Self::T, value_rep: impl AsRef<str>) -> anyhow::Result<()>;
}

enum MetricCatalogLens {
    Root(MetricCatalogRootLens),
    Health(JobHealthLens),
    Flow(FlowLens),
    Cluster(ClusterLens),
}

impl Lens for MetricCatalogLens {
    type T = MetricCatalog;

    fn get(&self, telemetry: &Self::T) -> String {
        match self {
            Self::Root(lens) => lens.get(telemetry),
            Self::Health(lens) => lens.get(&telemetry.health),
            Self::Flow(lens) => lens.get(&telemetry.flow),
            Self::Cluster(lens) => lens.get(&telemetry.cluster),
        }
    }

    fn set(&self, telemetry: &mut Self::T, value_rep: impl AsRef<str>) -> anyhow::Result<()> {
        match self {
            Self::Root(lens) => lens.set(telemetry, value_rep),
            Self::Health(lens) => lens.set(&mut telemetry.health, value_rep),
            Self::Flow(lens) => lens.set(&mut telemetry.flow, value_rep),
            Self::Cluster(lens) => lens.set(&mut telemetry.cluster, value_rep),
        }
    }
}

enum MetricCatalogRootLens {
    CorrelationId,
    Timestamp,
}

impl Lens for MetricCatalogRootLens {
    type T = MetricCatalog;

    fn get(&self, telemetry: &Self::T) -> String {
        match self {
            Self::CorrelationId => format!("{:#}", telemetry.correlation_id),
            Self::Timestamp => format!("{:#}", telemetry.timestamp),
        }
    }

    fn set(&self, telemetry: &mut Self::T, value_rep: impl AsRef<str>) -> anyhow::Result<()> {
        match self {
            Self::CorrelationId => {
                let snowflake = i64::from_str(value_rep.as_ref())?;
                telemetry.correlation_id = Id::new(snowflake, &IdPrettifier::<AlphabetCodec>::default());
            }
            Self::Timestamp => {
                telemetry.timestamp = Timestamp::from_str(value_rep.as_ref())?;
            }
        }

        Ok(())
    }
}

enum JobHealthLens {
    JobUptimeMillis,
    JobNrRestarts,
    JobNrCompletedCheckpoints,
    JobNrFailedCheckpoints,
}

impl Lens for JobHealthLens {
    type T = JobHealthMetrics;

    fn get(&self, telemetry: &Self::T) -> String {
        match self {
            Self::JobUptimeMillis => format!("{}", telemetry.job_uptime_millis),
            Self::JobNrRestarts => format!("{}", telemetry.job_nr_restarts),
            Self::JobNrCompletedCheckpoints => format!("{}", telemetry.job_nr_completed_checkpoints),
            Self::JobNrFailedCheckpoints => format!("{}", telemetry.job_nr_failed_checkpoints),
        }
    }

    fn set(&self, telemetry: &mut Self::T, value_rep: impl AsRef<str>) -> anyhow::Result<()> {
        match self {
            Self::JobUptimeMillis => telemetry.job_uptime_millis = i64::from_str(value_rep.as_ref())?,
            Self::JobNrRestarts => telemetry.job_nr_restarts = i64::from_str(value_rep.as_ref())?,
            Self::JobNrCompletedCheckpoints => {
                telemetry.job_nr_completed_checkpoints = i64::from_str(value_rep.as_ref())?
            }
            Self::JobNrFailedCheckpoints => telemetry.job_nr_failed_checkpoints = i64::from_str(value_rep.as_ref())?,
        }

        Ok(())
    }
}

enum FlowLens {
    RecordsInPerSec,
    RecordsOutPerSec,
    InputRecordsLagMax,
    InputMillisBehindLatest,
}

impl Lens for FlowLens {
    type T = FlowMetrics;

    fn get(&self, telemetry: &Self::T) -> String {
        match self {
            Self::RecordsInPerSec => format!("{}", telemetry.records_in_per_sec),
            Self::RecordsOutPerSec => format!("{}", telemetry.records_out_per_sec),
            Self::InputRecordsLagMax => telemetry
                .input_records_lag_max
                .map(|t| format!("{}", t))
                .unwrap_or(String::default()),
            Self::InputMillisBehindLatest => telemetry
                .input_millis_behind_latest
                .map(|t| format!("{}", t))
                .unwrap_or(String::default()),
        }
    }

    fn set(&self, telemetry: &mut Self::T, value_rep: impl AsRef<str>) -> anyhow::Result<()> {
        match self {
            Self::RecordsInPerSec => telemetry.records_in_per_sec = f64::from_str(value_rep.as_ref())?,
            Self::RecordsOutPerSec => telemetry.records_out_per_sec = f64::from_str(value_rep.as_ref())?,
            Self::InputRecordsLagMax => {
                telemetry.input_records_lag_max = if value_rep.as_ref().is_empty() {
                    None
                } else {
                    Some(i64::from_str(value_rep.as_ref())?)
                };
            }
            Self::InputMillisBehindLatest => {
                telemetry.input_millis_behind_latest = if value_rep.as_ref().is_empty() {
                    None
                } else {
                    Some(i64::from_str(value_rep.as_ref())?)
                };
            }
        }

        Ok(())
    }
}

enum ClusterLens {
    NrTaskManagers,
    TaskCpuLoad,
    TaskHeapMemoryUsed,
    TaskHeapMemoryCommitted,
    NrThreads,
    TaskNetworkInputQueueLen,
    TaskNetworkInputPoolUsage,
    TaskNetworkOutputQueueLen,
    TaskNetworkOutputPoolUsage,
}

impl Lens for ClusterLens {
    type T = ClusterMetrics;

    fn get(&self, telemetry: &Self::T) -> String {
        match self {
            Self::NrTaskManagers => format!("{}", telemetry.nr_task_managers),
            Self::TaskCpuLoad => format!("{}", telemetry.task_cpu_load),
            Self::TaskHeapMemoryUsed => format!("{}", telemetry.task_heap_memory_used),
            Self::TaskHeapMemoryCommitted => format!("{}", telemetry.task_heap_memory_committed),
            Self::NrThreads => format!("{}", telemetry.nr_threads),
            Self::TaskNetworkInputQueueLen => format!("{}", telemetry.task_network_input_queue_len),
            Self::TaskNetworkInputPoolUsage => format!("{}", telemetry.task_network_input_pool_usage),
            Self::TaskNetworkOutputQueueLen => format!("{}", telemetry.task_network_output_queue_len),
            Self::TaskNetworkOutputPoolUsage => format!("{}", telemetry.task_network_output_pool_usage),
        }
    }

    fn set(&self, telemetry: &mut Self::T, value_rep: impl AsRef<str>) -> anyhow::Result<()> {
        match self {
            Self::NrTaskManagers => telemetry.nr_task_managers = u16::from_str(value_rep.as_ref())?,
            Self::TaskCpuLoad => telemetry.task_cpu_load = f64::from_str(value_rep.as_ref())?,
            Self::TaskHeapMemoryUsed => telemetry.task_heap_memory_used = f64::from_str(value_rep.as_ref())?,
            Self::TaskHeapMemoryCommitted => telemetry.task_heap_memory_committed = f64::from_str(value_rep.as_ref())?,
            Self::NrThreads => telemetry.nr_threads = i64::from_str(value_rep.as_ref())?,
            Self::TaskNetworkInputQueueLen => {
                telemetry.task_network_input_queue_len = i64::from_str(value_rep.as_ref())?
            }
            Self::TaskNetworkInputPoolUsage => {
                telemetry.task_network_input_pool_usage = i64::from_str(value_rep.as_ref())?
            }
            Self::TaskNetworkOutputQueueLen => {
                telemetry.task_network_output_queue_len = i64::from_str(value_rep.as_ref())?
            }
            Self::TaskNetworkOutputPoolUsage => {
                telemetry.task_network_output_pool_usage = i64::from_str(value_rep.as_ref())?
            }
        }

        Ok(())
    }
}

fn make_metric_catalog(telemetry: MetricCatalog) -> anyhow::Result<MetricCatalog> {
    let facets: [(&str, Option<MetricCatalogLens>); 20] = [
        (
            "correlation_id",
            Some(MetricCatalogLens::Root(MetricCatalogRootLens::CorrelationId)),
        ),
        (
            "timestamp",
            Some(MetricCatalogLens::Root(MetricCatalogRootLens::Timestamp)),
        ),
        (
            "health.job_uptime_millis",
            Some(MetricCatalogLens::Health(JobHealthLens::JobUptimeMillis)),
        ),
        (
            "health.job_nr_restarts",
            Some(MetricCatalogLens::Health(JobHealthLens::JobNrRestarts)),
        ),
        (
            "health.job_nr_completed_checkpoints",
            Some(MetricCatalogLens::Health(JobHealthLens::JobNrCompletedCheckpoints)),
        ),
        (
            "health.job_nr_failed_checkpoints",
            Some(MetricCatalogLens::Health(JobHealthLens::JobNrFailedCheckpoints)),
        ),
        (
            "flow.records_in_per_sec",
            Some(MetricCatalogLens::Flow(FlowLens::RecordsInPerSec)),
        ),
        (
            "flow.records_out_per_sec",
            Some(MetricCatalogLens::Flow(FlowLens::RecordsOutPerSec)),
        ),
        (
            "flow.input_records_lag_max",
            Some(MetricCatalogLens::Flow(FlowLens::InputRecordsLagMax)),
        ),
        (
            "flow.input_millis_behind_latest",
            Some(MetricCatalogLens::Flow(FlowLens::InputMillisBehindLatest)),
        ),
        (
            "cluster.nr_task_managers",
            Some(MetricCatalogLens::Cluster(ClusterLens::NrTaskManagers)),
        ),
        (
            "cluster.task_cpu_load",
            Some(MetricCatalogLens::Cluster(ClusterLens::TaskCpuLoad)),
        ),
        (
            "cluster.task_heap_memory_used",
            Some(MetricCatalogLens::Cluster(ClusterLens::TaskHeapMemoryUsed)),
        ),
        (
            "cluster.task_heap_memory_committed",
            Some(MetricCatalogLens::Cluster(ClusterLens::TaskHeapMemoryCommitted)),
        ),
        (
            "cluster.nr_threads",
            Some(MetricCatalogLens::Cluster(ClusterLens::NrThreads)),
        ),
        (
            "cluster.task_network_input_queue_len",
            Some(MetricCatalogLens::Cluster(ClusterLens::TaskNetworkInputQueueLen)),
        ),
        (
            "cluster.task_network_input_pool_usage",
            Some(MetricCatalogLens::Cluster(ClusterLens::TaskNetworkInputPoolUsage)),
        ),
        (
            "cluster.task_network_output_queue_len",
            Some(MetricCatalogLens::Cluster(ClusterLens::TaskNetworkOutputQueueLen)),
        ),
        (
            "cluster.task_network_output_pool_usage",
            Some(MetricCatalogLens::Cluster(ClusterLens::TaskNetworkOutputPoolUsage)),
        ),
        ("Exit", None),
    ];

    #[tailcall]
    fn do_loop(
        mut telemetry: MetricCatalog, facets: &[(&str, Option<MetricCatalogLens>)],
    ) -> anyhow::Result<MetricCatalog> {
        let selections: Vec<&str> = facets.iter().map(|s| s.0).collect();
        let selection = FuzzySelect::with_theme(&*THEME)
            .with_prompt(
                r##"
                |Change any telemetry values (Esc to stop)?
                |Defaults are set to corresponding value in policy if possible.
                |"##
                .trim_margin_with("|")
                .unwrap(),
            )
            .items(selections.as_slice())
            .interact()?;

        let (label, telemetry_lens) = &facets[selection];
        match telemetry_lens {
            None => return Ok(telemetry),
            Some(lens) => {
                let value = Input::with_theme(&*THEME)
                    .with_prompt(*label)
                    .default(lens.get(&telemetry))
                    .interact()?;

                lens.set(&mut telemetry, value)?;
                make_metric_catalog(telemetry)
            }
        }
    }

    do_loop(telemetry, &facets)
}

fn edit_policy_settings<T>(phase: &str, settings: PolicySettings<T>) -> anyhow::Result<PolicySettings<T>>
where
    T: std::fmt::Debug + Default + Serialize + DeserializeOwned,
{
    eprintln!("{} settings are:\n", phase);

    let rep = ron_ser::to_string_pretty(&settings, PrettyConfig::default())?;
    // eprintln!("BBB: rep:\n||\n{}\n||\n", rep);
    let foo: PolicySettings<T> = ron::from_str(&rep)?;
    // eprintln!("CCC: {}\niFOO:{:?}", rep, foo);
    let settings_rep = if Confirm::with_theme(&*THEME)
        .with_prompt("Do you want to adjust them?")
        .interact()?
    {
        // eprintln!("DDD: editor...");
        Editor::new().edit(rep.as_str())?.unwrap_or(rep)
    } else {
        // eprintln!("DDDD: no editor...");
        rep
    };
    eprintln!("\nedited settings:\n{}", settings_rep);

    let result = ron::from_str(&settings_rep)?;
    Ok(result)
}

fn load_config_dialog() -> anyhow::Result<Settings> {
    let options = BASE_OPTIONS.get().expect("options not set");

    let mut builder: ConfigBuilder<DefaultState> = config::ConfigBuilder::default();
    builder = match options.config_path() {
        Some(ref path) => {
            eprintln!("\nUsing configuration at: {:?}", path);
            builder.add_source(Settings::make_explicit_config_source(path))
        }
        None => {
            let resources = Settings::resources();
            let basename = Settings::app_config_basename();
            eprintln!(
                "Using implicit configuration found at {:?} for basename: {}",
                resources, basename
            );
            builder = builder.add_source(Settings::make_implicit_app_config_sources(basename, &resources));

            // let environments = &[
            //     Environment::Production,
            //     Environment::Local,
            // ];
            //
            // let env_pos = FuzzySelect::with_theme(&THEME)
            //     .with_prompt("Which environment?")
            //     .default(0)
            //     .items(&environments[..])
            //     .interact()?;
            //
            // let env = *environments[env_pos];
            match std::env::var(Settings::env_app_environment()) {
                Ok(env_rep) => {
                    eprintln!(
                        "Looking for {} environment override configuration in: {:?}",
                        env_rep, resources
                    );
                    let env: Environment = env_rep.try_into()?;
                    builder.add_source(Settings::make_app_environment_source(env, &resources))
                }
                Err(std::env::VarError::NotPresent) => {
                    eprintln!(
                        "no environment variable override of base specified at envvar: {}",
                        Settings::env_app_environment()
                    );
                    builder
                }
                Err(err) => return Err(err.into()),
            }
        }
    };

    builder = match options.secrets_path() {
        Some(ref secrets) => {
            eprintln!("Using secrets at: {:?}", secrets);
            let s = Settings::make_secrets_source(secrets);
            builder.add_source(s)
        }
        None => {
            eprintln!("No secrets provided.");
            builder
        }
    };

    eprintln!(
        "Adding environment variable configuration based on prefix:{} path-separator:{}",
        Settings::environment_prefix(),
        Settings::environment_path_separator()
    );
    builder = builder.add_source(Settings::make_environment_variables_source());

    eprintln!("Adding CLI option overrides");
    builder = options.clone().load_overrides(builder)?;

    let config = builder.build()?;
    let settings: Settings = config.try_into()?;
    Ok(settings)
}
