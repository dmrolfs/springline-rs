// use crate::error::PlanError;
// use crate::flink::MetricCatalog;
// use serde::{Deserialize, Serialize};
// use smartcore::dataset::*;
// use smartcore::linalg::{naive::dense_matrix::DenseMatrix};
// use smartcore::model_selection::train_test_split;
// use std::fmt::Debug;
//
// // Logistic/Linear Regression
// use smartcore::linear::ridge_regression::{RidgeRegression, RidgeRegressionParameters};
// // Model performance
// use smartcore::linalg::BaseVector;
// use smartcore::math::num::RealNumber;
// use smartcore::metrics::{mean_absolute_error, mean_squared_error, r2};
// use super::{Workload, WorkflowForecast};
//
// //todo: smartcore needs to update nalgebra dependency to >0.27.2 to pass security audit
//
// #[derive(Debug)]
// pub struct RidgeRegressionWorkloadForecast(Dataset<f64, f64>);
//
// const F_TIMESTAMP: &'static str = "TIMESTAMP";
// const T_NR_RECS_IN_PER_SEC: &'static str = "NR_RECS_IN_PER_SEC";
//
// impl Default for RidgeRegressionWorkloadForecast {
//     fn default() -> Self {
//         Self(Dataset {
//             data: Vec::default(),
//             target: Vec::default(),
//             num_samples: 0,
//             num_features: 1,
//             feature_names: vec![F_TIMESTAMP].iter().map(|f| f.to_string()).collect(),
//             target_names: vec![T_NR_RECS_IN_PER_SEC].iter().map(|f| f.to_string()).collect(),
//             description: "Workload as a function of time".to_string(),
//         })
//     }
// }
//
// impl<T: WorkflowForecast> std::ops::Add<MetricCatalog> for T {
//     type Output = T;
//
//     fn add(mut self, rhs: MetricCatalog) -> Self::Output {
//         self.add_observation(rhs);
//         self
//     }
// }
//
// const MIN_SAMPLES_REQ: usize = 5;
//
// impl From<smartcore::error::Failed> for PlanError {
//     fn from(that: smartcore::error::Failed) -> Self {
//         PlanError::ForecastError(that.into())
//     }
// }
//
// impl WorkflowForecast for RidgeRegressionWorkloadForecast {
//     fn add_observation(&mut self, observation: MetricCatalog) {
//         //todo: likely want to keep dataset to finite set of most recent.
//
//         // can *only* do this since there is only one feature -
//         // more would need to consider num_features (see `Dataset::as_matrix`)
//         self.0.data.push(observations.timestamp.timestamp() as f64);
//         self.0.num_samples = self.0.data.len();
//
//         self.0.target.push(observations.flow.task_nr_records_in_per_sec);
//     }
//
//     fn clear(&mut self) {
//         self.0.data.clear();
//         self.0.num_samples = 0;
//         self.0.target.clear();
//     }
//
//     #[tracing::instrument(level = "info", skip(self))]
//     fn predict_workflow(&self) -> Result<Workload, PlanError> {
//         if self.0.num_samples < 5 {
//             return Ok(Workload::NotEnoughData);
//         }
//
//         let timestamps = DenseMatrix::from_array(self.0.num_samples, self.0.num_features,
// &self.0.data);
//
//         let workloads = &self.0.target;
//         let (ts_train, ts_test, workload_train, workload_test) = train_test_split(&timestamps,
// workloads, 0.2, true);         let workload_rr = RidgeRegression::fit(
//             &ts_train,
//             &workload_train,
//             RidgeRegressionParameters::default().with_alpha(0.5),
//         )?;
//
//         Self::predict_and_evaluate(workload_rr, ts_test, workload_test)
//     }
// }
//
// impl RidgeRegressionWorkloadForecast {
//     fn predict_and_evaluate(
//         workload_rr: RidgeRegression<f64, DenseMatrix<f64>>, ts_test: DenseMatrix<f64>,
// workload_test: Vec<f64>,     ) -> Result<Workload, PlanError> {
//         let avg_workload_test = workload_test.sum() / workload_test.len() as f64;
//         let avg_sq = avg_workload_test.square();
//
//         let workload_hat_rr = workload_rr.predict(&ts_test)?;
//         let mean_squared_error = mean_squared_error(&workload_test, &workload_hat_rr);
//         let mean_absolute_error = mean_absolute_error(&workload_test, &workload_hat_rr);
//         let r2 = r2(&workload_test, &workload_hat_rr);
//
//         let prediction = if (0.9 * avg_sq) < mean_squared_error {
//             Workload::HeuristicsExceedThreshold {
//                 // mean_squared_error,
//                 // mean_absolute_error,
//                 // r2,
//             }
//         } else {
//             let now = DenseMatrix::new(1, 1, vec![chrono::Utc::now().timestamp() as f64]);
//             let predicted_workload = workload_rr.predict(&now)?.get(0);
//             Workload::RecordsPerSecond(predicted_workload)
//         };
//
//         Ok(prediction)
//     }
// }
