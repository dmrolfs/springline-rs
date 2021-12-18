use std::path::PathBuf;

use clap::Parser;
use config::builder::DefaultState;
use config::ConfigBuilder;
use proctor::elements::PolicySettings;
use serde::{Deserialize, Serialize};
use settings_loader::common::http::HttpServerSettings;
use settings_loader::{Environment, LoadingOptions, SettingsError, SettingsLoader};

use crate::phases::decision::DecisionTemplateData;
use crate::phases::eligibility::policy::EligibilityTemplateData;

mod collection_settings;
mod engine_settings;
mod execution_settings;
mod governance_settings;
mod plan_settings;

pub use collection_settings::*;
pub use engine_settings::*;
pub use execution_settings::*;
pub use governance_settings::*;
pub use plan_settings::*;

pub type EligibilitySettings = PolicySettings<EligibilityTemplateData>;
pub type DecisionSettings = PolicySettings<DecisionTemplateData>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Settings {
    pub http: HttpServerSettings,
    #[serde(default)]
    pub engine: EngineSettings,
    #[serde(default)]
    pub collection: CollectionSettings,
    #[serde(default)]
    pub eligibility: EligibilitySettings,
    #[serde(default)]
    pub decision: DecisionSettings,
    #[serde(default)]
    pub plan: PlanSettings,
    #[serde(default)]
    pub governance: GovernanceSettings,
    pub execution: ExecutionSettings,
}

impl SettingsLoader for Settings {
    type Options = CliOptions;
}

#[derive(Parser, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[clap(author, version, about)]
// #[clap(version = "0.1.0", author = "Damon Rolfs")]
pub struct CliOptions {
    /// override environment-based configuration file to load.
    /// Default behavior is to load configuration based on `APP_ENVIRONMENT` envvar.
    #[clap(short, long)]
    pub config: Option<PathBuf>,

    /// specify path to secrets configuration file
    #[clap(short, long)]
    pub secrets: Option<PathBuf>,

    #[clap(short, long)]
    pub environment: Option<Environment>,

    /// Override default location from which to load configuration files. Default directory is
    /// ./resources.
    #[clap(short, long)]
    pub resources: Option<PathBuf>,

    /// Specify the machine id [0, 31) used in correlation id generation, overriding what may be set
    /// in an environment variable. This id should be unique for the entity type within a cluster
    /// environment. Different entity types can use the same machine id.
    #[clap(short, long)]
    pub machine_id: Option<i8>,

    /// Specify the node id [0, 31) used in correlation id generation, overriding what may be set
    /// in an environment variable. This id should be unique for the entity type within a cluster
    /// environment. Different entity types can use the same machine id.
    #[clap(short, long)]
    pub node_id: Option<i8>,
}

impl LoadingOptions for CliOptions {
    type Error = SettingsError;

    fn config_path(&self) -> Option<PathBuf> {
        self.config.clone()
    }

    fn resources_path(&self) -> Option<PathBuf> {
        self.resources.clone()
    }

    fn secrets_path(&self) -> Option<PathBuf> {
        self.secrets.clone()
    }

    fn load_overrides(&self, config: ConfigBuilder<DefaultState>) -> Result<ConfigBuilder<DefaultState>, Self::Error> {
        let config = match self.machine_id {
            None => config,
            Some(machine_id) => config.set_override("machine_id", machine_id as i64)?,
        };

        let config = match self.node_id {
            None => config,
            Some(node_id) => config.set_override("node_id", node_id as i64)?,
        };

        Ok(config)
    }

    fn environment_override(&self) -> Option<Environment> {
        self.environment
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

    use claim::assert_ok;
    use config::{Config, FileFormat};
    use lazy_static::lazy_static;
    use once_cell::sync::Lazy;
    use pretty_assertions::assert_eq;
    use proctor::elements::{PolicySource, TelemetryType, ToTelemetry};
    use proctor::phases::collection::SourceSetting;

    use super::*;
    use crate::phases::collection::flink::{Aggregation, FlinkScope, MetricOrder};
    use crate::phases::plan::{PerformanceRepositorySettings, PerformanceRepositoryType, SpikeSettings};

    lazy_static! {
        static ref SERIAL_TEST: Mutex<()> = Default::default();
    }

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
            .add_source(config::File::from(std::path::PathBuf::from("./tests/data/settings.ron")))
           // .add_source(config::File::from(std::path::PathBuf::from("./resources/application.ron")))
            .build());

        let expected = Settings {
            http: HttpServerSettings { host: "0.0.0.0".to_string(), port: 8000 },
            engine: Default::default(),
            collection: CollectionSettings {
                flink: FlinkSettings {
                    job_manager_scheme: "https".to_string(),
                    job_manager_host: "dr-flink-jm-0".to_string(),
                    job_manager_port: 8081,
                    metrics_initial_delay: Duration::from_secs(300),
                    metrics_interval: Duration::from_secs(15),
                    metric_orders: vec![
                        MetricOrder {
                            scope: FlinkScope::TaskManagers,
                            metric: "Status.JVM.Memory.NonHeap.Committed".to_string(),
                            agg: Aggregation::Max,
                            telemetry_path: "cluster.task_nonheap_memory_committed".to_string(),
                            telemetry_type: TelemetryType::Float,
                        },
                        MetricOrder {
                            scope: FlinkScope::Jobs,
                            metric: "uptime".to_string(),
                            agg: Aggregation::Min,
                            telemetry_path: "health.job_uptime_millis".to_string(),
                            telemetry_type: TelemetryType::Integer,
                        },
                    ],
                    headers: vec![(reqwest::header::ACCEPT.to_string(), "*.json".to_string())],
                    max_retries: 3,
                },
                sources: maplit::hashmap! {
                    "foo".to_string() => SourceSetting::Csv { path: PathBuf::from("../resources/bar.toml"), },
                },
            },
            eligibility: EligibilitySettings {
                policies: vec![
                    assert_ok!(PolicySource::from_template_file("../resources/eligibility.polar")),
                    assert_ok!(PolicySource::from_template_string(
                        "eligibility_basis",
                        r##"|
    |                        eligible(_, _context, length) if length = 13;
    |                        eligible(_item, context, c) if
    |                            c = context.custom() and
    |                            c.cat = "Otis" and
    |                            cut;
    |                    "##,
                    )),
                ],
                ..EligibilitySettings::default()
            },
            decision: DecisionSettings {
                policies: vec![
                    assert_ok!(PolicySource::from_complete_file("../resources/decision.polar")),
                    assert_ok!(PolicySource::from_complete_file("../resources/decision_basis.polar")),
                ],
                template_data: Some(DecisionTemplateData {
                    basis: "decision_basis".to_string(),
                    max_healthy_lag: Some(133.),
                    min_healthy_lag: 0.,
                    max_healthy_cpu_load: Some(0.7),
                    ..DecisionTemplateData::default()
                }),
                ..DecisionSettings::default()
            },
            plan: PlanSettings {
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
                    min_cluster_size: 0,
                    max_cluster_size: 10,
                    min_scaling_step: 2,
                    max_scaling_step: 6,
                    custom: maplit::hashmap! { "foo".to_string() => 17_i64.to_telemetry(), },
                },
            },
            execution: ExecutionSettings,
        };

        // let exp_rep = assert_ok!(ron::ser::to_string_pretty(&expected,
        // ron::ser::PrettyConfig::default())); assert_eq!(exp_rep.as_str(), "");

        let actual: Settings = assert_ok!(c.try_into());
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

        let actual: EligibilitySettings = assert_ok!(config.try_into());
        let expected = EligibilitySettings::default()
            .with_source(PolicySource::from_complete_file("./resources/eligibility.polar")?);
        assert_eq!(actual, expected);
        Ok(())
    }

    static SETTINGS: Lazy<Settings> = Lazy::new(|| Settings {
        http: HttpServerSettings { host: "0.0.0.0".to_string(), port: 8000 },
        engine: EngineSettings { machine_id: 7, node_id: 3 },
        collection: CollectionSettings {
            flink: FlinkSettings {
                job_manager_scheme: "https".to_string(),
                job_manager_host: "localhost".to_string(),
                job_manager_port: 8081,
                metrics_initial_delay: Duration::from_secs(300),
                metrics_interval: Duration::from_secs(15),
                metric_orders: vec![MetricOrder {
                    scope: FlinkScope::Kafka,
                    metric: "records-lag-max".to_string(),
                    agg: Aggregation::Value,
                    telemetry_path: "flow.input_records_lag_max".to_string(),
                    telemetry_type: TelemetryType::Integer,
                }],
                headers: vec![(reqwest::header::ACCEPT.to_string(), "*.json".to_string())],
                max_retries: 3,
            },
            sources: maplit::hashmap! {
                "foo".to_string() => SourceSetting::Csv { path: PathBuf::from("./resources/bar.toml"),},
            },
        },
        eligibility: EligibilitySettings::default()
            .with_source(assert_ok!(PolicySource::from_template_file(
                "./resources/eligibility.polar"
            )))
            .with_source(assert_ok!(PolicySource::from_template_file(
                "./resources/eligibility_basis.polar"
            )))
            .with_template_data(EligibilityTemplateData {
                basis: "eligibility_basis".to_string(),
                cooling_secs: Some(15 * 60),
                stable_secs: Some(15 * 60),
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
                max_healthy_lag: Some(133_f64),
                min_healthy_lag: 0_f64,
                max_healthy_cpu_load: Some(0.7),
                max_healthy_network_io: None,
                custom: HashMap::default(),
            }),
        plan: PlanSettings {
            min_scaling_step: 2,
            restart: Duration::from_secs(2 * 60),
            max_catch_up: Duration::from_secs(10 * 60),
            recovery_valid: Duration::from_secs(5 * 60),
            performance_repository: PerformanceRepositorySettings {
                storage: PerformanceRepositoryType::File,
                storage_path: Some("./tests/data/performance.data".to_string()),
            },
            window: 20,
            spike: SpikeSettings {
                std_deviation_threshold: 5.,
                influence: 0.75,
                length_threshold: 3,
            },
        },
        governance: GovernanceSettings {
            policy: GovernancePolicySettings::default().with_source(assert_ok!(PolicySource::from_complete_file(
                "./resources/governance.polar"
            ))),
            rules: GovernanceRuleSettings {
                min_cluster_size: 0,
                max_cluster_size: 20,
                min_scaling_step: 2,
                max_scaling_step: 10,
                custom: HashMap::default(),
            },
        },
        execution: ExecutionSettings,
    });

    #[test]
    fn test_settings_applications_load() -> anyhow::Result<()> {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_settings_applications_load");
        let _ = main_span.enter();

        let options = CliOptions {
            resources: Some("../resources".into()),
            ..CliOptions::default()
        };
        let before_env = Settings::load(&options);
        tracing::info!("from Settings::load: {:?}", before_env);
        let before_env = assert_ok!(before_env);
        assert_eq!(before_env.engine, EngineSettings { machine_id: 1, node_id: 1 });

        with_env_vars(
            "test_settings_applications_load",
            vec![
                ("APP_ENVIRONMENT", None),
                ("APP__ENGINE__MACHINE_ID", Some("7")),
                ("APP__ENGINE__NODE_ID", Some("3")),
            ],
            || {
                let actual: Settings = assert_ok!(Settings::load(&options));
                assert_eq!(actual.engine, EngineSettings { machine_id: 7, node_id: 3 });

                let expected = Settings {
                    collection: CollectionSettings {
                        flink: FlinkSettings {
                            metrics_initial_delay: Duration::from_secs(120),
                            headers: Vec::default(),
                            ..SETTINGS.collection.flink.clone()
                        },
                        ..SETTINGS.collection.clone()
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
            resources: Some("../resources".into()),
            ..CliOptions::default()
        };
        let before_env = Settings::load(&options);
        tracing::info!("from Settings::load: {:?}", before_env);
        let before_env = assert_ok!(before_env);
        assert_eq!(before_env.engine, EngineSettings { machine_id: 1, node_id: 1 });

        with_env_vars("test_local_load", vec![("APP_ENVIRONMENT", Some("local"))], || {
            let actual: Settings = assert_ok!(Settings::load(&options));
            assert_eq!(actual.engine, EngineSettings { machine_id: 1, node_id: 1 });

            let expected = Settings {
                http: HttpServerSettings {
                    host: "localhost".to_string(),
                    ..SETTINGS.http.clone()
                },
                engine: EngineSettings { machine_id: 1, node_id: 1 },
                collection: CollectionSettings {
                    flink: FlinkSettings {
                        metrics_initial_delay: Duration::from_secs(30),
                        headers: Vec::default(),
                        max_retries: 0,
                        ..SETTINGS.collection.flink.clone()
                    },
                    ..SETTINGS.collection.clone()
                },
                ..SETTINGS.clone()
            };

            assert_eq!(actual, expected);
        });

        Ok(())
    }

    #[test]
    fn test_production_load() -> anyhow::Result<()> {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_production_load");
        let _ = main_span.enter();

        let options = CliOptions {
            resources: Some("../resources".into()),
            ..CliOptions::default()
        };
        let before_env = Settings::load(&options);
        tracing::info!("from Settings::load: {:?}", before_env);
        let before_env = assert_ok!(before_env);
        assert_eq!(before_env.engine, EngineSettings { machine_id: 1, node_id: 1 });

        with_env_vars(
            "test_production_load",
            vec![("APP_ENVIRONMENT", Some("production"))],
            || {
                let actual: Settings = assert_ok!(Settings::load(&options));
                assert_eq!(actual.engine, EngineSettings { machine_id: 7, node_id: 3 });

                let expected = Settings {
                    collection: CollectionSettings {
                        flink: FlinkSettings {
                            job_manager_host: "dr-flink-jm-0".to_string(),
                            metrics_initial_delay: Duration::from_secs(120),
                            headers: Vec::default(),
                            ..SETTINGS.collection.flink.clone()
                        },
                        ..SETTINGS.collection.clone()
                    },
                    ..SETTINGS.clone()
                };

                assert_eq!(actual, expected);
            },
        );

        Ok(())
    }
}
