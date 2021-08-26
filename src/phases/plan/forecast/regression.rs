use std::fmt::Debug;

use nalgebra::{Matrix3, Matrix3x1};
use num_traits::pow;

use crate::phases::plan::WorkloadForecast;
use proctor::elements::{Point, RecordsPerSecond, Timestamp};
use proctor::error::PlanError;

#[derive(Debug)]
pub struct LinearRegression {
    pub slope: f64,
    pub y_intercept: f64,
    pub correlation_coefficient: f64,
}

impl LinearRegression {
    pub fn from_data(data: &[Point]) -> Self {
        let (n, sum_x, sum_y, sum_xy, sum_x2, sum_y2) = Self::components(data);
        let slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - pow(sum_x, 2));
        let y_intercept = (sum_y - slope * sum_x) / n;
        let correlation_coefficient = (n * sum_xy - sum_x * sum_y)
            / ((n * sum_x2 - pow(sum_x, 2)) * (n * sum_y2 - pow(sum_y, 2))).sqrt();
        Self {
            slope,
            y_intercept,
            correlation_coefficient,
        }
    }

    fn components(data: &[Point]) -> (f64, f64, f64, f64, f64, f64) {
        let (sum_x, sum_y, sum_xy, sum_x2, sum_y2) = data.iter().fold(
            (0., 0., 0., 0., 0.),
            |(acc_x, acc_y, acc_xy, acc_x2, acc_y2), (x, y)| {
                (
                    acc_x + x,
                    acc_y + y,
                    acc_xy + x * y,
                    acc_x2 + pow(*x, 2),
                    acc_y2 + pow(*y, 2),
                )
            },
        );

        let n = data.len() as f64;
        tracing::trace!(%sum_x, %sum_y, %sum_xy, %sum_x2, %sum_y2, %n, "intermediate linear regression calculations");
        (n, sum_x, sum_y, sum_xy, sum_x2, sum_y2)
    }
}

impl WorkloadForecast for LinearRegression {
    fn name(&self) -> &'static str {
        "LinearRegression"
    }

    fn workload_at(&self, timestamp: Timestamp) -> Result<RecordsPerSecond, PlanError> {
        let x: f64 = timestamp.into();
        Ok(RecordsPerSecond::new(self.slope * x + self.y_intercept).into())
    }

    fn total_records_between(&self, start: Timestamp, end: Timestamp) -> Result<f64, PlanError> {
        let integral = |x: f64| (self.slope / 2.) * pow(x, 2) + self.y_intercept * x;
        Ok(integral(end.into()) - integral(start.into()))
    }

    fn correlation_coefficient(&self) -> f64 {
        self.correlation_coefficient
    }
}

#[derive(Debug)]
pub struct QuadraticRegression {
    pub a: f64,
    pub b: f64,
    pub c: f64,
    pub correlation_coefficient: f64,
}

impl QuadraticRegression {
    pub fn from_data(data: &[Point]) -> Option<Self> {
        let n = data.len() as f64;
        let (sum_x, sum_y, sum_x2, sum_x3, sum_x4, sum_xy, sum_x2y) = data
            .iter()
            .map(|(x, y)| {
                (
                    x,
                    y,
                    pow(*x, 2),
                    pow(*x, 3),
                    pow(*x, 4),
                    x * y,
                    pow(*x, 2) * y,
                )
            })
            .fold(
                (0., 0., 0., 0., 0., 0., 0.),
                |(acc_x, acc_y, acc_x2, acc_x3, acc_x4, acc_xy, acc_x2y),
                 (x, y, x2, x3, x4, xy, x2y)| {
                    (
                        acc_x + x,
                        acc_y + y,
                        acc_x2 + x2,
                        acc_x3 + x3,
                        acc_x4 + x4,
                        acc_xy + xy,
                        acc_x2y + x2y,
                    )
                },
            );

        tracing::trace!(%sum_x, %sum_y, %sum_x2, %sum_x3, %sum_x4, %sum_xy, %sum_x2y, %n, "intermediate quadratic regression calculations");

        let m_x = Matrix3::new(
            sum_x4, sum_x3, sum_x2, sum_x3, sum_x2, sum_x, sum_x2, sum_x, n,
        );
        tracing::trace!(X=?m_x, "Matrix X");

        let m_y = Matrix3x1::new(sum_x2y, sum_xy, sum_y);
        tracing::trace!(Y=?m_y, "Matrix Y");

        let decomp = m_x.lu();

        // .ok_or(PlanError::RegressionFailed(
        // Utc.timestamp_millis(data[data.len() - 1].0 as i64),
        // ))?;
        decomp.solve(&m_y).map(|coefficients| {
            let a = coefficients[(0, 0)];
            let b = coefficients[(1, 0)];
            let c = coefficients[(2, 0)];

            let y_mean = sum_y / n;
            let sse = data.iter().fold(0., |acc, (x, y)| {
                acc + pow(y - (a * pow(*x, 2) + b * x + c), 2)
            });

            let sst = data.iter().fold(0., |acc, (_, y)| acc + pow(y - y_mean, 2));

            let correlation_coefficient = (1. - sse / sst).sqrt();

            Self {
                a,
                b,
                c,
                correlation_coefficient,
            }
        })
    }
}

impl WorkloadForecast for QuadraticRegression {
    fn name(&self) -> &'static str {
        "QuadraticRegression"
    }

    fn workload_at(&self, timestamp: Timestamp) -> Result<RecordsPerSecond, PlanError> {
        let x: f64 = timestamp.into();
        Ok(RecordsPerSecond::new(
            self.a * pow(x, 2) + self.b * x + self.c,
        ))
    }

    fn total_records_between(&self, start: Timestamp, end: Timestamp) -> Result<f64, PlanError> {
        let integral = |x: f64| self.a / 3. * pow(x, 3) + self.b / 2. * pow(x, 2) + self.c * x;
        Ok(integral(end.into()) - integral(start.into()))
    }

    fn correlation_coefficient(&self) -> f64 {
        self.correlation_coefficient
    }
}

#[cfg(test)]
mod tests {
    use approx::assert_relative_eq;
    use claim::{assert_ok, assert_some};

    use super::*;

    #[test]
    fn test_quadratic_regression_coefficients() -> anyhow::Result<()> {
        // example take from https://www.symbolab.com/solver/system-of-equations-calculator/9669a%2B1225b%2B165c%3D5174.3%2C%201225a%2B165b%2B25c%3D809.7%2C%20165a%2B25b%2B5c%3D167.1#
        let data = vec![(1., 32.5), (3., 37.3), (5., 36.4), (7., 32.4), (9., 28.5)];

        let actual = assert_some!(QuadraticRegression::from_data(&data));
        assert_relative_eq!(actual.a, (-41. / 112.), epsilon = 1.0e-10);
        assert_relative_eq!(actual.b, (2_111. / 700.), epsilon = 1.0e-10);
        assert_relative_eq!(actual.c, (85_181. / 2_800.), epsilon = 1.0e-10);
        Ok(())
    }

    #[test]
    fn test_quadratic_regression_integration() -> anyhow::Result<()> {
        let data = vec![(1., 32.5), (3., 37.3), (5., 36.4), (7., 32.4), (9., 28.5)];
        let regression = assert_some!(QuadraticRegression::from_data(&data));
        let actual = regression.total_records_between(1.25.into(), 8.4.into())?;
        assert_relative_eq!(actual, 249.468_468_824_405, epsilon = 1.0e-10);
        Ok(())
    }

    #[test]
    fn test_quadratic_regression_prediction() -> anyhow::Result<()> {
        // example take from https://www.symbolab.com/solver/system-of-equations-calculator/9669a%2B1225b%2B165c%3D5174.3%2C%201225a%2B165b%2B25c%3D809.7%2C%20165a%2B25b%2B5c%3D167.1#
        let data = vec![(1., 32.5), (3., 37.3), (5., 36.4), (7., 32.4), (9., 28.5)];
        let regression = assert_some!(QuadraticRegression::from_data(&data));
        let actual = assert_ok!(regression.workload_at(11.into()));
        let expected = (-41. / 112.) * pow(11., 2) + (2111. / 700.) * 11. + (85181. / 2800.);
        assert_relative_eq!(actual, expected.into(), epsilon = 1.0e-10);
        Ok(())
    }

    #[test]
    fn test_linear_regression_coefficients() -> anyhow::Result<()> {
        // example taken from https://tutorme.com/blog/post/quadratic-regression/
        let data = vec![(1., 1.), (2., 3.), (3., 2.), (4., 3.), (5., 5.)];
        let actual = LinearRegression::from_data(&data);
        assert_relative_eq!(actual.slope, 0.8, epsilon = 1.0e-10);
        assert_relative_eq!(actual.y_intercept, 0.4, epsilon = 1.0e-10);
        Ok(())
    }

    #[test]
    fn test_linear_regression_prediction() -> anyhow::Result<()> {
        // example taken from https://tutorme.com/blog/post/quadratic-regression/
        let data = vec![(1., 1.), (2., 3.), (3., 2.), (4., 3.), (5., 5.)];
        let regression = LinearRegression::from_data(&data);

        let accuracy = 0.69282037;
        assert_relative_eq!(
            regression.correlation_coefficient(),
            0.853,
            epsilon = 1.0e-3
        );

        let actual = assert_ok!(regression.workload_at(3.into()));
        assert_relative_eq!(actual, 3.0.into(), epsilon = accuracy);
        Ok(())
    }

    #[test]
    fn test_linear_regression_integration() -> anyhow::Result<()> {
        let data = vec![(1., 1.), (2., 3.), (3., 2.), (4., 3.), (5., 5.)];
        let regression = LinearRegression::from_data(&data);
        let actual = regression.total_records_between(1.25.into(), 4.89.into())?;
        assert_relative_eq!(actual, 10.395_84, epsilon = 1.0e-5);
        Ok(())
    }
}
