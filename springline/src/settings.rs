use std::path::PathBuf;

use clap::Parser;
use config::builder::{ConfigBuilder, DefaultState};
use proctor::elements::PolicySettings;
use serde::{Deserialize, Serialize};
use settings_loader::common::http::HttpServerSettings;
use settings_loader::{Environment, LoadingOptions, SettingsError, SettingsLoader};

use crate::phases::decision::DecisionTemplateData;
use crate::phases::eligibility::EligibilityTemplateData;

mod action_settings;
mod engine_settings;
mod flink_settings;
mod governance_settings;
mod kubernetes_settings;
mod plan_settings;
mod sensor_settings;

pub use action_settings::{
    ActionSettings, FlinkActionSettings, FlinkRestartSettings, TaskmanagerContext,
};
pub use engine_settings::EngineSettings;
pub use flink_settings::FlinkSettings;
pub use governance_settings::{
    GovernancePolicySettings, GovernanceRuleSettings, GovernanceSettings,
};
pub use kubernetes_settings::{KubernetesSettings, LoadKubeConfig};
pub use plan_settings::PlanSettings;
pub use sensor_settings::{FlinkSensorSettings, SensorSettings};

pub type EligibilitySettings = PolicySettings<EligibilityTemplateData>;
pub type DecisionSettings = PolicySettings<DecisionTemplateData>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(PartialEq))]
pub struct Settings {
    #[serde(default)]
    pub prometheus: PrometheusSettings,

    pub http: HttpServerSettings,

    #[serde(default)]
    pub flink: FlinkSettings,

    #[serde(default)]
    pub kubernetes: KubernetesSettings,

    #[serde(default)]
    pub engine: EngineSettings,

    #[serde(default)]
    pub sensor: SensorSettings,

    #[serde(default)]
    pub eligibility: EligibilitySettings,

    #[serde(default)]
    pub decision: DecisionSettings,

    #[serde(default)]
    pub plan: PlanSettings,

    #[serde(default)]
    pub governance: GovernanceSettings,

    pub action: ActionSettings,

    pub context_stub: ContextStubSettings,
}

impl SettingsLoader for Settings {
    type Options = CliOptions;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrometheusSettings {
    /// The port on which the metrics server should listen. Default is 9898..
    pub port: u16,
}

impl Default for PrometheusSettings {
    fn default() -> Self {
        Self { port: 9898 }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(PartialEq))]
pub struct ContextStubSettings {
    pub all_sinks_healthy: bool,
    // pub cluster_is_deploying: bool,
    // #[serde(with = "proctor::serde")]
    // pub cluster_last_deployment: DateTime<Utc>,
}

#[derive(Parser, Clone, Debug, Default, Serialize, Deserialize)]
#[clap(author, version, about)]
// #[clap(version = "0.1.0", author = "Damon Rolfs")]
#[cfg_attr(test, derive(PartialEq))]
pub struct CliOptions {
    /// Explicit configuration to load, bypassing inferred configuration load mechanism. If this
    /// option is used, the application + environment will be ignored; however, secrets, env var,
    /// and explicit overrides will still be used.
    ///
    /// Default behavior is to infer-load configuration based on `APP_ENVIRONMENT` envvar.
    #[clap(short, long, value_name = "PATH_TO_CONFIG_FILE")]
    pub config: Option<PathBuf>,

    /// specify path to secrets configuration file
    #[clap(long, value_name = "PATH_TO_SECRETS_FILE")]
    pub secrets: Option<PathBuf>,

    /// specify the environment configuration override used in inferred configuration load.
    #[clap(short = 'e', long = "env")]
    pub environment: Option<Environment>,

    /// Override filesystem path used to search for application and environment configuration files.
    /// Directories are separated by the ':' character.
    /// Default path is "./resources".
    #[clap(short = 's', long = "search-path", value_name = "SETTINGS_SEARCH_PATH")]
    pub settings_search_path: Option<String>,

    /// Flink jobmanager scheme, optionally overrides flink.job_manager_uri_scheme setting.
    #[clap(long = "scheme", value_name = "SCHEME")]
    pub jobmanager_scheme: Option<String>,

    /// Flink jobmanager host, optionally overrides flink.job_manager_host setting.
    #[clap(short = 'h', long = "host", value_name = "HOST")]
    pub jobmanager_host: Option<String>,

    /// Flink jobmanager port, optionally overrides flink.job_manager_port setting.
    #[clap(short = 'p', long = "port", value_name = "PORT")]
    pub jobmanager_port: Option<u16>,

    /// Specify the machine id [0, 31) used in correlation id generation, overriding what may be set
    /// in an environment variable. This id should be unique for the entity type within a cluster
    /// environment. Different entity types can use the same machine id.
    /// Optionally overrides the engine.machine_id setting.
    #[clap(short, long, value_name = "[0, 31)")]
    pub machine_id: Option<i8>,

    /// Specify the node id [0, 31) used in correlation id generation, overriding what may be set
    /// in an environment variable. This id should be unique for the entity type within a cluster
    /// environment. Different entity types can use the same machine id.
    /// Optionally override the engine.node_id setting.
    #[clap(short, long, value_name = "[0, 31)")]
    pub node_id: Option<i8>,
}

const DEFAULT_SEARCH_PATH: &str = "./resources";

impl LoadingOptions for CliOptions {
    type Error = SettingsError;

    fn config_path(&self) -> Option<PathBuf> {
        self.config.clone()
    }

    fn implicit_search_paths(&self) -> Vec<PathBuf> {
        let search_path = self.settings_search_path.as_deref().unwrap_or(DEFAULT_SEARCH_PATH);
        search_path.split(':').map(PathBuf::from).collect()
    }

    fn secrets_path(&self) -> Option<PathBuf> {
        self.secrets.clone()
    }

    fn load_overrides(
        &self, config: ConfigBuilder<DefaultState>,
    ) -> Result<ConfigBuilder<DefaultState>, Self::Error> {
        let config = match self.jobmanager_scheme.as_ref() {
            None => config,
            Some(scheme) => config.set_override("flink.job_manager_uri_scheme", scheme.clone())?,
        };

        let config = match self.jobmanager_host.as_ref() {
            None => config,
            Some(host) => config.set_override("flink.job_manager_host", host.clone())?,
        };

        let config = match self.jobmanager_port.as_ref() {
            None => config,
            Some(port) => config.set_override("flink.job_manager_port", i64::from(*port))?,
        };

        let config = match self.machine_id {
            None => config,
            Some(machine_id) => config.set_override("machine_id", i64::from(machine_id))?,
        };

        let config = match self.node_id {
            None => config,
            Some(node_id) => config.set_override("node_id", i64::from(node_id))?,
        };

        Ok(config)
    }

    fn environment_override(&self) -> Option<Environment> {
        self.environment.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::env::VarError;
    use std::panic::{RefUnwindSafe, UnwindSafe};
    use std::sync::Mutex;
    use std::time::Duration;
    use std::{env, panic};

    use claim::*;
    use config::{Config, FileFormat};
    use once_cell::sync::Lazy;
    use pretty_assertions::assert_eq;
    use proctor::elements::{PolicySource, TelemetryType, ToTelemetry};
    use proctor::phases::sense::clearinghouse::{CacheTtl, TelemetryCacheSettings};
    use proctor::phases::sense::SensorSetting;

    use super::*;
    use crate::flink::RestoreMode;
    use crate::kubernetes::{KubernetesApiConstraints, KubernetesDeployResource};
    use crate::phases::plan::{
        PerformanceRepositorySettings, PerformanceRepositoryType, SpikeSettings,
    };
    use crate::phases::sense::flink::Aggregation::Sum;
    use crate::phases::sense::flink::{
        Aggregation, DerivativeCombinator, FlinkScope, MetricOrder, MetricSpec, PlanPositionSpec,
    };
    use crate::settings::action_settings::SavepointSettings;
    use crate::settings::sensor_settings::FlinkSensorSettings;
    use Aggregation::{Max, Min, Value};
    use TelemetryType::{Float, Integer};

    static SERIAL_TEST: Lazy<Mutex<()>> = Lazy::new(|| Default::default());

    /// Sets environment variables to the given value for the duration of the closure.
    /// Restores the previous values when the closure completes or panics, before unwinding the
    /// panic.
    pub fn with_env_vars<F>(label: &str, kvs: Vec<(&str, Option<&str>)>, closure: F)
    where
        F: Fn() + UnwindSafe + RefUnwindSafe,
    {
        let guard = SERIAL_TEST.lock().unwrap();
        let mut old_kvs: Vec<(&str, Result<String, VarError>)> = Vec::new();
        for (k, v) in kvs {
            let old_v = env::var(k);
            old_kvs.push((k, old_v));
            match v {
                None => env::remove_var(k),
                Some(v) => env::set_var(k, v),
            }
        }
        eprintln!("W_ENV[{}]: OLD_KVS: {:?}", label, old_kvs);
        let old_kvs_2 = old_kvs.clone();

        match panic::catch_unwind(|| {
            closure();
        }) {
            Ok(_) => {
                eprintln!("W_END[{}]: OK - resetting env to: {:?}", label, old_kvs);
                for (k, v) in old_kvs {
                    reset_env(k, v);
                }
            },
            Err(err) => {
                eprintln!("W_END[{}]: Err - resetting env to: {:?}", label, old_kvs);
                for (k, v) in old_kvs {
                    reset_env(k, v);
                }
                drop(guard);
                panic::resume_unwind(err);
            },
        };
        for (k, v) in old_kvs_2 {
            eprintln!(
                "W_END[{}] RESET ACTUAL: {:?}:{:?} expected:{:?}",
                label,
                k,
                env::var(k),
                v
            );
        }
    }

    fn reset_env(k: &str, old: Result<String, VarError>) {
        if let Ok(v) = old {
            env::set_var(k, v);
        } else {
            env::remove_var(k);
        }
    }

    #[test]
    fn test_basic_load() {
        let c = assert_ok!(config::Config::builder()
            .add_source(config::File::from(std::path::PathBuf::from("./tests/data/settings.yaml")))
           // .add_source(config::File::from(std::path::PathBuf::from("./resources/application.ron")))
            .build());

        let expected = Settings {
            http: HttpServerSettings { host: "0.0.0.0".to_string(), port: 8000 },
            prometheus: PrometheusSettings::default(),
            flink: FlinkSettings {
                job_manager_uri_scheme: "http".to_string(),
                job_manager_host: "dr-flink-jm-0".to_string(),
                job_manager_port: 8081,
                headers: vec![(reqwest::header::ACCEPT.to_string(), "*.json".to_string())],
                max_retries: 3,
                pool_idle_timeout: None,
                pool_max_idle_per_host: None,
                ..FlinkSettings::default()
            },
            kubernetes: KubernetesSettings::default(),
            engine: Default::default(),
            sensor: SensorSettings {
                clearinghouse: TelemetryCacheSettings::default(),
                flink: FlinkSensorSettings {
                    metrics_initial_delay: Duration::from_secs(300),
                    metrics_interval: Duration::from_secs(15),
                    metric_orders: vec![
                        MetricOrder::TaskManager {
                            metric: MetricSpec::new(
                                "Status.JVM.Memory.NonHeap.Committed",
                                Max,
                                "cluster.task_nonheap_memory_committed",
                                Float,
                            ),
                        },
                        MetricOrder::Job {
                            metric: MetricSpec::new(
                                "uptime",
                                Min,
                                "health.job_uptime_millis",
                                Integer,
                            ),
                        },
                        MetricOrder::Operator {
                            name: "Source: Data Stream".to_string(),
                            position: PlanPositionSpec::Source,
                            metric: MetricSpec::new(
                                "records-lag-max",
                                Sum,
                                "flow.source_records_lag_max",
                                Integer,
                            ),
                        },
                        MetricOrder::Operator {
                            name: "Data Stream".to_string(),
                            position: PlanPositionSpec::Source,
                            metric: MetricSpec::new(
                                "assigned-partitions",
                                Sum,
                                "flow.source_assigned_partitions",
                                Integer,
                            ),
                        },
                        MetricOrder::Derivative {
                            scope: FlinkScope::Operator,
                            position: PlanPositionSpec::Source,
                            telemetry_path: "flow.source_total_lag".to_string(),
                            telemetry_type: Float,
                            telemetry_lhs: "flow.source_records_lag_max".to_string(),
                            telemetry_rhs: "flow.source_assigned_partitions".to_string(),
                            combinator: DerivativeCombinator::Product,
                            agg: Sum,
                        },
                    ],
                },
                sensors: maplit::hashmap! {
                    "foo".to_string() => SensorSetting::Csv { path: PathBuf::from("../resources/bar.toml"), },
                },
            },
            eligibility: EligibilitySettings {
                policies: vec![
                    assert_ok!(PolicySource::from_template_file(
                        "../resources/eligibility.polar"
                    )),
                    assert_ok!(PolicySource::from_template_string(
                        "eligibility_ext",
                        r##"|eligible(_, _context, length) if length = 13;
                        |eligible(_item, context, c) if
                        |  c = context.custom() and
                        |  c.cat = "Otis" and
                        |  cut;
                        |"##,
                    )),
                ],
                ..EligibilitySettings::default()
            },
            decision: DecisionSettings {
                policies: vec![
                    assert_ok!(PolicySource::from_complete_file(
                        "../resources/decision.polar"
                    )),
                    assert_ok!(PolicySource::from_complete_file(
                        "../resources/decision_basis.polar"
                    )),
                ],
                template_data: Some(DecisionTemplateData {
                    basis: "decision_basis".to_string(),
                    max_healthy_lag: Some(133.),
                    max_healthy_relative_lag_velocity: Some(1.75),
                    max_healthy_cpu_load: Some(0.7),
                    ..DecisionTemplateData::default()
                }),
                ..DecisionSettings::default()
            },
            plan: PlanSettings {
                min_cluster_size: 1,
                min_scaling_step: 2,
                restart: Duration::from_secs(120),
                max_catch_up: Duration::from_secs(600),
                recovery_valid: Duration::from_secs(300),
                performance_repository: PerformanceRepositorySettings {
                    storage: PerformanceRepositoryType::File,
                    storage_path: Some("./tests/data/performance.data".to_string()),
                },
                window: 20,
                spike: SpikeSettings {
                    std_deviation_threshold: 5.0,
                    influence: 0.75,
                    length_threshold: 3,
                },
            },
            governance: GovernanceSettings {
                policy: GovernancePolicySettings {
                    policies: vec![assert_ok!(PolicySource::from_complete_file(
                        "../resources/governance.polar"
                    ))],
                    ..GovernancePolicySettings::default()
                },
                rules: GovernanceRuleSettings {
                    min_cluster_size: 1,
                    max_cluster_size: 10,
                    min_scaling_step: 2,
                    max_scaling_step: 6,
                    custom: maplit::hashmap! { "foo".to_string() => 17_i64.to_telemetry(), },
                },
            },
            action: ActionSettings {
                action_timeout: Duration::from_secs(600),
                taskmanager: TaskmanagerContext {
                    label_selector: "app=flink,component=taskmanager".to_string(),
                    deploy_resource: KubernetesDeployResource::StatefulSet {
                        name: "dr-springline-tm".to_string(),
                    },
                    kubernetes_api: KubernetesApiConstraints {
                        api_timeout: Duration::from_secs(290),
                        polling_interval: Duration::from_secs(10),
                    },
                },
                flink: FlinkActionSettings {
                    polling_interval: Duration::from_secs(9),
                    savepoint: SavepointSettings {
                        directory: Some("s3://path/to/savepoints".to_string()),
                        ..SavepointSettings::default()
                    },
                    restart: FlinkRestartSettings {
                        operation_timeout: Duration::from_secs(300),
                        restore_mode: Some(RestoreMode::Claim),
                        program_args: Some(vec!["--zed=98".to_string(), "--alpha=boo".to_string()]),
                        ..FlinkRestartSettings::default()
                    },
                    ..FlinkActionSettings::default()
                },
            },
            context_stub: ContextStubSettings {
                all_sinks_healthy: true,
                // cluster_is_deploying: false,
                // cluster_last_deployment: assert_ok!(
                //     Utc.datetime_from_str("2020-03-01T04:28:07Z", proctor::serde::date::FORMAT)
                // ),
            },
        };

        // let exp_rep = assert_ok!(serde_yaml::to_string(&expected));
        // assert_eq!(exp_rep.as_str(), "");
        // let exp_rep = assert_ok!(ron::ser::to_string_pretty(&expected, ron::ser::PrettyConfig::default()));
        // assert_eq!(exp_rep.as_str(), "");
        // let exp_actual: Settings = assert_ok!(ron::from_str(&exp_rep));
        // assert_eq!(exp_actual, expected);

        let actual: Settings = assert_ok!(c.try_deserialize());
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_load_eligibility_settings() -> anyhow::Result<()> {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_load_eligibility_settings");
        let _ = main_span.enter();

        let config = assert_ok!(Config::builder()
            .add_source(config::File::from_str(
                r###"
                (
                    required_subscription_fields: [],
                    optional_subscription_fields: [],
                    policies: [
                        (source: "file", policy: (path:"./resources/eligibility.polar")),
                    ],
                )
                "###,
                FileFormat::Ron
            ))
            .build());

        tracing::info!(?config, "eligibility config loaded.");

        let actual: EligibilitySettings = assert_ok!(config.try_deserialize());
        let expected = EligibilitySettings::default().with_source(
            PolicySource::from_complete_file("./resources/eligibility.polar")?,
        );
        assert_eq!(actual, expected);
        Ok(())
    }

    static SETTINGS: Lazy<Settings> = Lazy::new(|| Settings {
        http: HttpServerSettings { host: "0.0.0.0".to_string(), port: 8000 },
        prometheus: PrometheusSettings::default(),
        flink: FlinkSettings {
            job_manager_uri_scheme: "https".to_string(),
            job_manager_host: "localhost".to_string(),
            job_manager_port: 8081,
            headers: vec![(reqwest::header::ACCEPT.to_string(), "*.json".to_string())],
            max_retries: 3,
            pool_idle_timeout: None,
            pool_max_idle_per_host: None,
            ..FlinkSettings::default()
        },
        kubernetes: KubernetesSettings::default(),
        engine: EngineSettings {
            machine_id: 7,
            node_id: 3,
            ..EngineSettings::default()
        },
        sensor: SensorSettings {
            clearinghouse: TelemetryCacheSettings {
                ttl: CacheTtl {
                    never_expire: maplit::hashset! {
                        "all_sinks_healthy".to_string(),
                        "cluster.is_rescaling".to_string(),
                        "cluster.is_deploying".to_string(),
                        "cluster.last_deployment".to_string(),
                    },
                    ..CacheTtl::default()
                },
                ..TelemetryCacheSettings::default()
            },
            flink: FlinkSensorSettings {
                metrics_initial_delay: Duration::from_secs(300),
                metrics_interval: Duration::from_secs(15),
                metric_orders: vec![MetricOrder::Operator {
                    name: "Source: my data".to_string(),
                    position: PlanPositionSpec::NotSource,
                    metric: MetricSpec::new(
                        "records-lag-max",
                        Value,
                        "flow.source_records_lag_max",
                        Integer,
                    ),
                }],
            },
            sensors: maplit::hashmap! {
                "foo".to_string() => SensorSetting::Csv { path: PathBuf::from("./resources/bar.toml"),},
            },
        },
        eligibility: EligibilitySettings::default()
            .with_source(assert_ok!(PolicySource::from_template_file(
                "./resources/eligibility.polar"
            )))
            .with_source(assert_ok!(PolicySource::from_template_file(
                "./resources/eligibility_ext.polar"
            )))
            .with_template_data(EligibilityTemplateData {
                policy_extension: Some("eligibility_ext".to_string()),
                cooling_secs: Some(5 * 60),
                stable_secs: Some(5 * 60),
                custom: HashMap::default(),
            }),
        decision: DecisionSettings::default()
            .with_source(assert_ok!(PolicySource::from_template_file(
                "./resources/decision.polar"
            )))
            .with_source(assert_ok!(PolicySource::from_template_file(
                "./resources/decision_basis.polar"
            )))
            .with_template_data(DecisionTemplateData {
                basis: "decision_basis".to_string(),
                max_healthy_relative_lag_velocity: Some(3.0),
                min_task_utilization: Some(0.6),
                evaluate_duration_secs: Some(60),
                ..DecisionTemplateData::default()
            }),
        plan: PlanSettings {
            min_cluster_size: 1,
            min_scaling_step: 2,
            restart: Duration::from_secs(2 * 60),
            max_catch_up: Duration::from_secs(10 * 60),
            recovery_valid: Duration::from_secs(5 * 60),
            performance_repository: PerformanceRepositorySettings {
                storage: PerformanceRepositoryType::File,
                storage_path: Some("./tmp".to_string()),
            },
            window: 20,
            spike: SpikeSettings {
                std_deviation_threshold: 5.,
                influence: 0.75,
                length_threshold: 3,
            },
        },
        governance: GovernanceSettings {
            policy: GovernancePolicySettings::default().with_source(assert_ok!(
                PolicySource::from_complete_file("./resources/governance.polar")
            )),
            rules: GovernanceRuleSettings {
                min_cluster_size: 1,
                max_cluster_size: 20,
                min_scaling_step: 2,
                max_scaling_step: 4,
                custom: HashMap::default(),
            },
        },
        action: ActionSettings {
            action_timeout: Duration::from_secs(600),
            taskmanager: TaskmanagerContext {
                label_selector: "component=taskmanager,release=dr-springline".to_string(),
                deploy_resource: KubernetesDeployResource::StatefulSet {
                    name: "dr-springline-tm".to_string(),
                },
                kubernetes_api: KubernetesApiConstraints {
                    api_timeout: Duration::from_secs(295),
                    polling_interval: Duration::from_secs(5),
                },
            },
            flink: FlinkActionSettings {
                savepoint: SavepointSettings {
                    directory: Some("s3://path/to/savepoints".to_string()),
                    ..SavepointSettings::default()
                },
                restart: FlinkRestartSettings {
                    restore_mode: Some(RestoreMode::Claim),
                    ..FlinkRestartSettings::default()
                },
                ..FlinkActionSettings::default()
            },
        },
        context_stub: ContextStubSettings { all_sinks_healthy: true },
    });

    #[test]
    fn test_settings_applications_load() -> anyhow::Result<()> {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_settings_applications_load");
        let _ = main_span.enter();

        let options = CliOptions {
            settings_search_path: Some("../resources".into()),
            ..CliOptions::default()
        };

        with_env_vars(
            "test_settings_applications_load",
            vec![
                ("APP_ENVIRONMENT", None),
                ("APP__FLINK__JOB_MANAGER_HOST", Some("host.lima.internal")),
                ("APP__ENGINE__MACHINE_ID", Some("17")),
                ("APP__ENGINE__NODE_ID", Some("13")),
            ],
            || {
                let actual: Settings = assert_ok!(Settings::load(&options));
                assert_eq!(
                    actual.engine,
                    EngineSettings {
                        machine_id: 17,
                        node_id: 13,
                        ..EngineSettings::default()
                    }
                );

                let expected = Settings {
                    engine: EngineSettings {
                        machine_id: 17,
                        node_id: 13,
                        ..EngineSettings::default()
                    },
                    flink: FlinkSettings {
                        job_manager_uri_scheme: "http".to_string(),
                        job_manager_host: "host.lima.internal".to_string(),
                        pool_idle_timeout: Some(Duration::from_secs(60)),
                        pool_max_idle_per_host: Some(5),
                        headers: Vec::default(),
                        ..SETTINGS.flink.clone()
                    },
                    kubernetes: KubernetesSettings {
                        client: LoadKubeConfig::Infer,
                        ..SETTINGS.kubernetes.clone()
                    },
                    sensor: SensorSettings {
                        flink: FlinkSensorSettings {
                            metrics_initial_delay: Duration::from_secs(0),
                            metric_orders: vec![MetricOrder::Job {
                                metric: MetricSpec::new(
                                    "lastCheckpointDuration",
                                    Max,
                                    "health.last_checkpoint_duration",
                                    Integer,
                                ),
                            }],
                            ..SETTINGS.sensor.flink.clone()
                        },
                        sensors: HashMap::default(),
                        ..SETTINGS.sensor.clone()
                    },
                    eligibility: EligibilitySettings {
                        template_data: Some(EligibilityTemplateData {
                            cooling_secs: Some(300),
                            ..SETTINGS.eligibility.template_data.clone().unwrap()
                        }),
                        ..SETTINGS.eligibility.clone()
                    },
                    decision: DecisionSettings {
                        template_data: Some(DecisionTemplateData {
                            evaluate_duration_secs: Some(120),
                            ..SETTINGS.decision.template_data.clone().unwrap()
                        }),
                        ..SETTINGS.decision.clone()
                    },
                    plan: PlanSettings {
                        performance_repository: PerformanceRepositorySettings {
                            storage: PerformanceRepositoryType::Memory,
                            storage_path: None,
                        },
                        ..SETTINGS.plan.clone()
                    },
                    action: ActionSettings {
                        taskmanager: TaskmanagerContext {
                            kubernetes_api: KubernetesApiConstraints {
                                api_timeout: Duration::from_secs(290),
                                polling_interval: Duration::from_secs(5),
                                ..SETTINGS.action.taskmanager.kubernetes_api.clone()
                            },
                            ..SETTINGS.action.taskmanager.clone()
                        },
                        flink: FlinkActionSettings {
                            polling_interval: Duration::from_secs(1),
                            savepoint: SavepointSettings {
                                directory: None,
                                ..SETTINGS.action.flink.savepoint.clone()
                            },
                            ..SETTINGS.action.flink.clone()
                        },
                        ..SETTINGS.action.clone()
                    },
                    ..SETTINGS.clone()
                };

                assert_eq!(actual, expected);
            },
        );

        Ok(())
    }

    #[test]
    fn test_local_load() -> anyhow::Result<()> {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_local_load");
        let _ = main_span.enter();

        let options = CliOptions {
            settings_search_path: Some("../resources".into()),
            ..CliOptions::default()
        };
        let before_env = Settings::load(&options);
        tracing::info!("from Settings::load: {:?}", before_env);
        let before_env = assert_ok!(before_env);
        assert_eq!(
            before_env.plan.performance_repository,
            PerformanceRepositorySettings {
                storage: PerformanceRepositoryType::Memory,
                storage_path: None,
            }
        );

        with_env_vars(
            "test_local_load",
            vec![("APP_ENVIRONMENT", Some("local"))],
            || {
                let actual: Settings = assert_ok!(Settings::load(&options));
                assert_eq!(
                    actual.engine,
                    EngineSettings {
                        machine_id: 1,
                        node_id: 1,
                        ..EngineSettings::default()
                    }
                );

                let expected = Settings {
                    http: HttpServerSettings {
                        host: "localhost".to_string(),
                        ..SETTINGS.http.clone()
                    },
                    flink: FlinkSettings {
                        job_manager_uri_scheme: "http".to_string(),
                        job_manager_host: "localhost".to_string(),
                        headers: Vec::default(),
                        max_retries: 0,
                        pool_idle_timeout: Some(Duration::from_secs(60)),
                        pool_max_idle_per_host: Some(5),
                        ..SETTINGS.flink.clone()
                    },
                    engine: EngineSettings {
                        machine_id: 1,
                        node_id: 1,
                        ..EngineSettings::default()
                    },
                    sensor: SensorSettings {
                        flink: FlinkSensorSettings {
                            metrics_initial_delay: Duration::from_secs(10),
                            metric_orders: vec![MetricOrder::Job {
                                metric: MetricSpec::new(
                                    "lastCheckpointDuration",
                                    Max,
                                    "health.last_checkpoint_duration",
                                    Integer,
                                ),
                            }],
                            ..SETTINGS.sensor.flink.clone()
                        },
                        sensors: HashMap::default(),
                        ..SETTINGS.sensor.clone()
                    },
                    eligibility: EligibilitySettings {
                        template_data: Some(EligibilityTemplateData {
                            cooling_secs: Some(60),
                            ..SETTINGS.eligibility.template_data.clone().unwrap()
                        }),
                        ..SETTINGS.eligibility.clone()
                    },
                    decision: DecisionSettings {
                        template_data: Some(DecisionTemplateData {
                            max_healthy_relative_lag_velocity: Some(3.3),
                            max_healthy_lag: Some(133_f64),
                            max_healthy_cpu_load: Some(0.0006),
                            // max_healthy_heap_memory_load: Some(0.5),
                            ..SETTINGS.decision.template_data.clone().unwrap()
                        }),
                        ..SETTINGS.decision.clone()
                    },
                    action: ActionSettings {
                        action_timeout: Duration::from_secs(60),
                        taskmanager: TaskmanagerContext {
                            kubernetes_api: KubernetesApiConstraints {
                                api_timeout: Duration::from_secs(290),
                                polling_interval: Duration::from_secs(5),
                                ..SETTINGS.action.taskmanager.kubernetes_api.clone()
                            },
                            ..SETTINGS.action.taskmanager.clone()
                        },
                        flink: FlinkActionSettings {
                            polling_interval: Duration::from_secs(3),
                            savepoint: SavepointSettings {
                                directory: Some("s3a://my/flink/savepoints".into()),
                                ..SETTINGS.action.flink.savepoint.clone()
                            },
                            ..SETTINGS.action.flink.clone()
                        },
                        ..SETTINGS.action.clone()
                    },
                    ..SETTINGS.clone()
                };

                assert_eq!(actual, expected);
            },
        );

        Ok(())
    }

    #[test]
    fn test_production_load() -> anyhow::Result<()> {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_production_load");
        let _ = main_span.enter();

        let options = CliOptions {
            settings_search_path: Some("../resources".into()),
            ..CliOptions::default()
        };
        let before_env = Settings::load(&options);
        tracing::info!("from Settings::load: {:?}", before_env);
        let before_env = assert_ok!(before_env);
        assert_eq!(
            before_env.plan.performance_repository,
            PerformanceRepositorySettings {
                storage: PerformanceRepositoryType::Memory,
                storage_path: None,
            }
        );

        with_env_vars(
            "test_production_load",
            vec![("APP_ENVIRONMENT", Some("production"))],
            || {
                let actual: Settings = assert_ok!(Settings::load(&options));
                assert_eq!(
                    actual.engine,
                    EngineSettings {
                        machine_id: 7,
                        node_id: 3,
                        ..EngineSettings::default()
                    }
                );

                let expected = Settings {
                    http: HttpServerSettings {
                        host: "0.0.0.0".to_string(),
                        ..SETTINGS.http.clone()
                    },
                    flink: FlinkSettings {
                        job_manager_uri_scheme: "http".to_string(),
                        job_manager_host: "dr-springline-jm-0".to_string(),
                        pool_idle_timeout: Some(Duration::from_secs(60)),
                        pool_max_idle_per_host: Some(5),
                        headers: Vec::default(),
                        ..SETTINGS.flink.clone()
                    },
                    kubernetes: KubernetesSettings {
                        patch_settle_timeout: Duration::from_secs(60),
                        ..SETTINGS.kubernetes.clone()
                    },
                    eligibility: EligibilitySettings {
                        template_data: Some(EligibilityTemplateData {
                            // cooling_secs: Some(60),
                            ..SETTINGS.eligibility.template_data.clone().unwrap()
                        }),
                        ..SETTINGS.eligibility.clone()
                    },
                    sensor: SensorSettings {
                        flink: FlinkSensorSettings {
                            metrics_initial_delay: Duration::from_secs(0),
                            metric_orders: vec![MetricOrder::Job {
                                metric: MetricSpec::new(
                                    "lastCheckpointDuration",
                                    Max,
                                    "health.last_checkpoint_duration",
                                    Integer,
                                ),
                            }],
                            ..SETTINGS.sensor.flink.clone()
                        },
                        sensors: HashMap::default(),
                        ..SETTINGS.sensor.clone()
                    },
                    decision: DecisionSettings {
                        template_data: Some(DecisionTemplateData {
                            evaluate_duration_secs: Some(120),
                            ..SETTINGS.decision.template_data.clone().unwrap()
                        }),
                        ..SETTINGS.decision.clone()
                    },
                    plan: PlanSettings {
                        performance_repository: PerformanceRepositorySettings {
                            storage: PerformanceRepositoryType::File,
                            storage_path: Some("/var/tmp".to_string()),
                        },
                        ..SETTINGS.plan.clone()
                    },
                    action: ActionSettings {
                        taskmanager: TaskmanagerContext {
                            kubernetes_api: KubernetesApiConstraints {
                                api_timeout: Duration::from_secs(290),
                                polling_interval: Duration::from_secs(5),
                                ..SETTINGS.action.taskmanager.kubernetes_api.clone()
                            },
                            ..SETTINGS.action.taskmanager.clone()
                        },
                        flink: FlinkActionSettings {
                            savepoint: SavepointSettings {
                                directory: None,
                                ..SETTINGS.action.flink.savepoint.clone()
                            },
                            ..SETTINGS.action.flink.clone()
                        },
                        ..SETTINGS.action.clone()
                    },
                    ..SETTINGS.clone()
                };

                assert_eq!(actual, expected);
            },
        );

        Ok(())
    }
}
