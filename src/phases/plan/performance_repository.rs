use std::fmt::{Debug, Display};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
#[cfg(test)]
use mockall::{automock, predicate::*};
use serde::{Deserialize, Serialize};
use serde_json::error::Category;
use serde_with::{DeserializeFromStr, SerializeDisplay};

use crate::phases::plan::PerformanceHistory;
use proctor::error::PlanError;

pub fn make_performance_repository(
    settings: PerformanceRepositorySettings,
) -> Result<Box<dyn PerformanceRepository>, PlanError> {
    match settings.storage {
        PerformanceRepositoryType::Memory => Ok(Box::new(PerformanceMemoryRepository::default())),
        PerformanceRepositoryType::File => {
            let path = settings
                .storage_path
                .unwrap_or("performance_history.data".to_string());
            Ok(Box::new(PerformanceFileRepository::new(path)))
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PerformanceRepositorySettings {
    pub storage: PerformanceRepositoryType,
    pub storage_path: Option<String>,
}

#[derive(Debug, Display, SerializeDisplay, DeserializeFromStr)]
pub enum PerformanceRepositoryType {
    Memory,
    File,
}

impl FromStr for PerformanceRepositoryType {
    type Err = PlanError;

    fn from_str(rep: &str) -> Result<Self, Self::Err> {
        match rep.to_lowercase().as_str() {
            "memory" => Ok(PerformanceRepositoryType::Memory),
            "file" => Ok(PerformanceRepositoryType::File),
            s => Err(PlanError::ParseError(format!(
                "unknown performance repository type, {}",
                s
            ))),
        }
    }
}

#[cfg_attr(test, automock)]
#[async_trait]
pub trait PerformanceRepository: Debug + Sync + Send {
    async fn load(&self, job_name: &str) -> Result<Option<PerformanceHistory>, PlanError>;
    async fn save(
        &mut self,
        job_name: &str,
        performance_history: &PerformanceHistory,
    ) -> Result<(), PlanError>;
    async fn close(self: Box<Self>) -> Result<(), PlanError>;
}

#[derive(Debug, Default)]
pub struct PerformanceMemoryRepository(Arc<DashMap<String, PerformanceHistory>>);

impl PerformanceMemoryRepository {}

#[async_trait]
impl PerformanceRepository for PerformanceMemoryRepository {
    #[tracing::instrument(level = "info", skip(self))]
    async fn load(&self, job_name: &str) -> Result<Option<PerformanceHistory>, PlanError> {
        let performance_history = self.0.get(job_name).map(|a| a.clone());
        tracing::debug!(?performance_history, "memory loaded performance history.");
        Ok(performance_history)
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn save(
        &mut self,
        job_name: &str,
        performance_history: &PerformanceHistory,
    ) -> Result<(), PlanError> {
        let old = self
            .0
            .insert(job_name.to_string(), performance_history.clone());
        tracing::debug!(?old, "replacing performance history in repository.");
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn close(mut self: Box<Self>) -> Result<(), PlanError> {
        self.0.clear();
        Ok(())
    }
}

#[derive(Debug)]
pub struct PerformanceFileRepository {
    root_path: PathBuf,
}

impl PerformanceFileRepository {
    pub fn new(root: impl AsRef<str>) -> Self {
        Self {
            root_path: PathBuf::from(root.as_ref()),
        }
    }

    fn file_name_for(&self, job_name: &str) -> String {
        format!("{}.json", job_name)
    }

    #[tracing::instrument(level = "info")]
    fn path_for(&self, filename: &str) -> PathBuf {
        let mut path = self.root_path.clone();
        path.push(filename);
        tracing::debug!(?path, "looking for performance repository at file path.");
        path
    }

    #[tracing::instrument(level = "info", skip(path), fields(path=?path.as_ref()))]
    fn file_for(&self, path: impl AsRef<Path>, read_write: bool) -> Result<File, std::io::Error> {
        let mut options = OpenOptions::new();
        options.read(true);
        if read_write {
            options.write(true).create(true);
        }
        Ok(options.open(path)?)
    }
}

#[async_trait]
impl PerformanceRepository for PerformanceFileRepository {
    #[tracing::instrument(level = "info", skip(self))]
    async fn load(&self, job_name: &str) -> Result<Option<PerformanceHistory>, PlanError> {
        let performance_history_path = self.path_for(self.file_name_for(job_name).as_str());
        let performance_history = self.file_for(performance_history_path.clone(), false);
        tracing::debug!(?performance_history, "file_for: {}", job_name);

        match performance_history {
            Ok(history_file) => {
                let reader = BufReader::new(history_file);
                let ph = match serde_json::from_reader(reader) {
                    Ok(a) => Ok(Some(a)),
                    Err(err) if err.classify() == Category::Eof => {
                        tracing::debug!(
                            ?performance_history_path,
                            "performance history empty, creating new."
                        );
                        Ok(None)
                    }
                    Err(err) => Err(err),
                };
                tracing::debug!(performance_history=?ph, "file loaded performance history.");

                Ok(ph?)
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn save(
        &mut self,
        job_name: &str,
        performance_history: &PerformanceHistory,
    ) -> Result<(), PlanError> {
        let performance_history_path = self.path_for(self.file_name_for(job_name).as_str());
        let performance_history_file = self.file_for(performance_history_path.clone(), true)?;
        let writer = BufWriter::new(performance_history_file);
        serde_json::to_writer(writer, performance_history)?;
        tracing::debug!(?performance_history_path, "saved performance history data");
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn close(self: Box<Self>) -> Result<(), PlanError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use claim::{assert_none, assert_ok, assert_some};
    use pretty_assertions::assert_eq;
    use tokio_test::block_on;

    use super::*;
    use crate::phases::plan::Benchmark;

    async fn do_test_repository<'a>(
        repo: &mut impl PerformanceRepository,
        jobs_a_b: (&'a str, &'a str),
    ) -> anyhow::Result<()> {
        let (name_a, name_b) = jobs_a_b;
        let actual = repo.load(name_a).await;
        let actual = assert_ok!(actual);
        assert_none!(actual);

        let mut ph = PerformanceHistory::default();
        ph.add_upper_benchmark(Benchmark::new(4, 3.5.into()));
        let actual = repo.save(name_a, &ph).await;
        assert_ok!(actual);

        ph.add_upper_benchmark(Benchmark::new(4, 21.3.into()));
        ph.add_upper_benchmark(Benchmark::new(12, 37.324.into()));
        let actual = repo.save(name_b, &ph).await;
        assert_ok!(actual);

        let actual_a = repo.load(name_a).await;
        let actual_a = assert_ok!(actual_a);
        let actual_a = assert_some!(actual_a);
        let mut expected = PerformanceHistory::default();
        expected.add_upper_benchmark(Benchmark::new(4, 3.5.into()));
        assert_eq!(actual_a, expected);

        let actual_b = repo.load(name_b).await;
        let actual_b = assert_ok!(actual_b);
        let actual_b = assert_some!(actual_b);
        let mut expected = PerformanceHistory::default();
        expected.add_upper_benchmark(Benchmark::new(4, 21.3.into()));
        expected.add_upper_benchmark(Benchmark::new(12, 37.324.into()));
        assert_eq!(actual_b, expected);

        let actual = repo.load("dummy").await;
        let actual = assert_ok!(actual);
        assert_none!(actual);

        Ok(())
    }

    #[test]
    fn test_performance_memory_repository() -> anyhow::Result<()> {
        lazy_static::initialize(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_performance_memory_repository");
        let _main_span_guard = main_span.enter();

        let mut repo = PerformanceMemoryRepository::default();
        block_on(async {
            let test_result = do_test_repository(&mut repo, ("AAA", "BBB")).await;
            assert_ok!(test_result);

            let actual = Box::new(repo).close().await;
            assert_ok!(actual);
            Ok(())
        })
    }

    #[test]
    fn test_performance_file_repository() -> anyhow::Result<()> {
        lazy_static::initialize(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_performance_file_repository");
        let _main_span_guard = main_span.enter();

        let aaa = "AAA";
        let bbb = "BBB";
        let mut repo = PerformanceFileRepository::new("./target");
        let (aaa_path, bbb_path) = block_on(async {
            (
                repo.path_for(repo.file_name_for(aaa).as_str()),
                repo.path_for(repo.file_name_for(bbb).as_str()),
            )
        });

        block_on(async {
            let result = do_test_repository(&mut repo, (aaa, bbb)).await;
            assert_ok!(result);

            let actual = Box::new(repo).close().await;
            assert_ok!(actual);
        });

        std::fs::remove_file(aaa_path)?;
        std::fs::remove_file(bbb_path)?;
        Ok(())
    }
}