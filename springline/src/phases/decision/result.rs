use std::cmp::Ordering;
use std::convert::TryFrom;
use std::fmt::{self, Debug};

use crate::phases;
use itertools::Itertools;
use pretty_snowflake::Id;
use proctor::elements::{PolicyOutcome, QueryResult, TelemetryType, TelemetryValue, ToTelemetry};
use proctor::error::{DecisionError, TelemetryError};
use proctor::graph::stage::{self, ThroughStage};
use proctor::Correlation;

use crate::phases::decision::{
    DecisionContext, DecisionData, ScaleDirection, DECISION_SCALING_DECISION_COUNT_METRIC,
};
use crate::phases::UNSPECIFIED;

pub const DECISION_DIRECTION: &str = "direction";
pub const SCALE_UP: &str = "up";
pub const SCALE_DOWN: &str = "down";
pub const NO_ACTION: &str = "no action";

pub fn make_decision_transform(
    name: impl Into<String>, window: u32,
) -> impl ThroughStage<PolicyOutcome<DecisionData, DecisionContext>, DecisionResult<DecisionData>> {
    stage::Map::new(
        name,
        move |outcome: PolicyOutcome<DecisionData, DecisionContext>| {
            let transform_span = tracing::trace_span!(
                "distill policy outcome into action",
                item=?outcome.item, policy_results=?outcome.policy_results,
            );
            let _guard = transform_span.enter();

            let (result, reason): (DecisionResult<DecisionData>, Option<String>) = if outcome
                .passed()
            {
                match get_direction_and_reason(&outcome.policy_results) {
                    Ok((ScaleDirection::None, reason)) => {
                        if let Some(ref r) = reason {
                            DECISION_SCALING_DECISION_COUNT_METRIC
                                .with_label_values(&[NO_ACTION, r.as_str()])
                                .inc();
                        }
                        (DecisionResult::NoAction(outcome.item), reason)
                    },
                    Ok((ScaleDirection::Up, reason)) => {
                        let reason_rep = reason.as_deref().unwrap_or(UNSPECIFIED);
                        DECISION_SCALING_DECISION_COUNT_METRIC
                            .with_label_values(&[SCALE_UP, reason_rep])
                            .inc();
                        (DecisionResult::ScaleUp(outcome.item), reason)
                    },
                    Ok((ScaleDirection::Down, reason)) => {
                        let reason_rep = reason.as_deref().unwrap_or(UNSPECIFIED);
                        DECISION_SCALING_DECISION_COUNT_METRIC
                            .with_label_values(&[SCALE_DOWN, reason_rep])
                            .inc();
                        (DecisionResult::ScaleDown(outcome.item), reason)
                    },
                    Err(err) => {
                        tracing::error!(error=?err, "error in decision policy - taking NoAction");
                        (DecisionResult::NoAction(outcome.item), None)
                    },
                }
            } else {
                tracing::debug!("item did not pass context policy review.");
                (DecisionResult::NoAction(outcome.item), None)
            };

            log_data_for_reason(result.direction(), reason.as_deref(), result.item(), window);
            result
        },
    )
}

pub fn get_direction_and_reason(
    query_results: &QueryResult,
) -> Result<(ScaleDirection, Option<String>), DecisionError> {
    let directions = query_results
        .binding(DECISION_DIRECTION)
        .map_err(|err| DecisionError::Stage(err.into()))?;

    let tally = do_tally_binding_voted(directions);
    tracing::debug!(direction_tally=?tally, "Tallying decision directions");

    match tally.first() {
        Some((direction, _)) => match direction.as_str() {
            SCALE_UP => Ok((
                ScaleDirection::Up,
                Some(phases::get_outcome_reason(query_results)),
            )),
            SCALE_DOWN => Ok((
                ScaleDirection::Down,
                Some(phases::get_outcome_reason(query_results)),
            )),
            rep => Err(DecisionError::Binding {
                key: DECISION_DIRECTION.to_string(),
                value: rep.to_string(),
            }),
        },
        None => Ok((ScaleDirection::None, None)),
    }
}

fn do_tally_binding_voted(ballots: Vec<String>) -> Vec<(String, usize)> {
    let mut grouped = vec![];
    for (direction, votes) in &ballots.into_iter().group_by(|d| d.clone()) {
        grouped.push((direction, votes.count()));
    }

    grouped.sort_by(|lhs, rhs| {
        let cmp = rhs.1.cmp(&lhs.1);
        if cmp != Ordering::Equal {
            cmp
        } else if lhs.0 == SCALE_UP {
            Ordering::Greater
        } else {
            Ordering::Less
        }
    });

    grouped
}

const RELATIVE_LAG_VELOCITY: &str = "relative_lag_velocity";
const LOW_UTILIZATION_AND_ZERO_LAG: &str = "low_utilization_and_zero_lag";
const LOW_UTILIZATION_AND_IDLE_TELEMETRY: &str = "low_utilization_and_idle_telemetry";
const TOTAL_LAG: &str = "total_lag";
const SOURCE_BACKPRESSURE: &str = "source_backpressure";
const NO_REASON: &str = "no_reason_given";

#[allow(clippy::cognitive_complexity)]
fn log_data_for_reason(
    direction: ScaleDirection, reason: Option<&str>, item: &DecisionData, window: u32,
) {
    let message = format!(
        "{direction} decision made for: {}",
        reason.unwrap_or(NO_REASON)
    );

    match (direction, reason) {
        (ScaleDirection::None, _) => {},

        (_, Some(RELATIVE_LAG_VELOCITY)) => {
            let source_records_lag_max = item.flow_source_records_lag_max_rolling_average(window);
            let source_assigned_partitions =
                item.flow_source_assigned_partitions_rolling_average(window);
            let relative_lag_velocity = item.flow_source_relative_lag_change_rate(window);

            tracing::info!(
                correlation=%item.correlation(),
                %window, ?source_records_lag_max, ?source_assigned_partitions, %relative_lag_velocity,
                message
            );
        },

        (_, Some(LOW_UTILIZATION_AND_ZERO_LAG)) => {
            let source_records_lag_max = item.flow_source_records_lag_max_rolling_average(window);
            let source_assigned_partitions =
                item.flow_source_assigned_partitions_rolling_average(window);
            let nonsource_utilization = item.flow_task_utilization_rolling_average(window);
            let total_lag = item.flow_source_total_lag_rolling_average(window);

            tracing::info!(
                correlation=%item.correlation(),
                %window, ?source_records_lag_max, ?source_assigned_partitions, %nonsource_utilization, %total_lag,
                message
            );
        },

        (_, Some(LOW_UTILIZATION_AND_IDLE_TELEMETRY)) => {
            let source_records_lag_max = item.flow.source_total_lag;
            let nonsource_utilization = item.flow_task_utilization_rolling_average(window);
            let source_back_pressure =
                item.flow_source_back_pressured_time_millis_per_sec_rolling_average(window);

            tracing::info!(
                correlation=%item.correlation(),
                %window, ?source_records_lag_max, %nonsource_utilization, %source_back_pressure,
                message
            );
        },

        (_, Some(TOTAL_LAG)) => {
            let source_records_lag_max = item.flow_source_records_lag_max_rolling_average(window);
            let source_assigned_partitions =
                item.flow_source_assigned_partitions_rolling_average(window);
            let total_lag = item.flow_source_total_lag_rolling_average(window);

            tracing::info!(
                correlation=%item.correlation(),
                %window, ?source_records_lag_max, ?source_assigned_partitions, %total_lag,
                message
            );
        },

        (_, Some(SOURCE_BACKPRESSURE)) => {
            let source_backpressure =
                item.flow_source_back_pressured_time_millis_per_sec_rolling_average(window);
            tracing::info!(correlation=%item.correlation(), %window, %source_backpressure, message);
        },

        _ => tracing::info!(correlation=%item.correlation(), ?reason, ?item, message),
    }
}

// pub fn make_decision_transform<T, C, S>(name: S) -> impl ThroughStage<PolicyOutcome<T, C>, DecisionResult<T>>
// where
//     T: AppData + Correlation + PartialEq,
//     C: ProctorContext,
//     S: Into<String>,
// {
//     let name = name.into();
//     stage::Map::new(name.clone(), move |outcome: PolicyOutcome<T, C>| {
//         let transform_span = tracing::trace_span!(
//             "distill policy outcome into action",
//             item=?outcome.item, policy_results=?outcome.policy_results
//         );
//         let _transform_span_guard = transform_span.enter();
//
//         if outcome.passed() {
//             outcome
//                 .policy_results
//                 .binding(DECISION_DIRECTION)
//                 .map(|directions: Vec<String>| {
//                     let mut grouped: Vec<(String, usize)> = vec![];
//                     for (direction, votes) in &directions.into_iter().group_by(|d| d.clone()) {
//                         grouped.push((direction, votes.count()));
//                     }
//                     grouped.sort_by(|lhs, rhs| {
//                         let cmp = rhs.1.cmp(&lhs.1);
//                         if cmp != Ordering::Equal {
//                             cmp
//                         } else if lhs.0 == SCALE_UP {
//                             Ordering::Greater
//                         } else {
//                             Ordering::Less
//                         }
//                     });
//
//                     let result = grouped
//                         .first()
//                         .map(|(d, _)| match d.as_str() {
//                             SCALE_UP => {
//                                 let reason = outcome
//                                     .policy_results
//                                     .bindings
//                                     .get(REASON)
//                                     .and_then(|rs| rs.first())
//                                     .map(|r| r.to_string())
//                                     .unwrap_or_else(|| "unspecified".into());
//
//                                 DECISION_SCALING_DECISION_COUNT_METRIC
//                                     .with_label_values(&[SCALE_UP, &reason])
//                                     .inc();
//
//                                 DecisionResult::ScaleUp(outcome.item.clone())
//                             },
//                             SCALE_DOWN => {
//                                 let reason = outcome
//                                     .policy_results
//                                     .bindings
//                                     .get(REASON)
//                                     .and_then(|rs| rs.first())
//                                     .map(|r| r.to_string())
//                                     .unwrap_or_else(|| "unspecified".into());
//
//                                 DECISION_SCALING_DECISION_COUNT_METRIC
//                                     .with_label_values(&[SCALE_DOWN, &reason])
//                                     .inc();
//
//                                 DecisionResult::ScaleDown(outcome.item.clone())
//                             },
//                             direction => {
//                                 tracing::warn!(%direction, "unknown direction determined by policy - NoAction");
//                                 DecisionResult::NoAction(outcome.item.clone())
//                             },
//                         })
//                         .unwrap_or_else(|| {
//                             tracing::warn!("no direction determined by policy - NoAction");
//                             DecisionResult::NoAction(outcome.item.clone())
//                         });
//
//                     tracing::debug!(
//                         item = ?outcome.item,
//                         ?result, direction_votes=%grouped.iter().map(|(d, v)| format!("{d}:{v}")).join(","),
//                         "DMR(KEEP): evaluating {name} policy results."
//                     );
//
//                     result
//                 })
//                 .unwrap_or_else(|err| {
//                     tracing::error!(error=?err, "policy failed to assess direction - NoAction.");
//                     DecisionResult::NoAction(outcome.item)
//                 })
//         } else {
//             tracing::debug!("item did not pass context policy review.");
//             DecisionResult::NoAction(outcome.item)
//         }
//     })
// }

const T_ITEM: &str = "item";
const T_SCALE_DECISION: &str = "scale_decision";

pub enum DecisionResult<T> {
    ScaleUp(T),
    ScaleDown(T),
    NoAction(T),
}

impl<T: Clone> Clone for DecisionResult<T> {
    fn clone(&self) -> Self {
        match self {
            Self::ScaleUp(val) => Self::ScaleUp(val.clone()),
            Self::ScaleDown(val) => Self::ScaleDown(val.clone()),
            Self::NoAction(val) => Self::NoAction(val.clone()),
        }
    }
}

impl<T: Copy> Copy for DecisionResult<T> {}

impl<T: PartialEq> PartialEq for DecisionResult<T> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::ScaleUp(lhs), Self::ScaleUp(rhs)) => lhs == rhs,
            (Self::ScaleDown(lhs), Self::ScaleDown(rhs)) => lhs == rhs,
            (Self::NoAction(lhs), Self::NoAction(rhs)) => lhs == rhs,
            _ => false,
        }
    }
}

impl<T: Eq + PartialEq> Eq for DecisionResult<T> {}

impl<T: Debug> fmt::Debug for DecisionResult<T>
where
    T: Correlation,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ScaleUp(_) => write!(f, "ScaleUp({:?})", self.correlation()),
            Self::ScaleDown(_) => write!(f, "ScaleDown({:?})", self.correlation()),
            Self::NoAction(_) => write!(f, "NoAction({:?})", self.correlation()),
        }
    }
}

impl<T> fmt::Display for DecisionResult<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::ScaleUp(_) => "scale_up",
            Self::ScaleDown(_) => "scale_down",
            Self::NoAction(_) => "no_action",
        };

        write!(f, "{}", label)
    }
}

impl<T> DecisionResult<T> {
    pub fn new(item: T, decision_rep: &str) -> Self {
        match decision_rep {
            SCALE_UP => Self::ScaleUp(item),
            SCALE_DOWN => Self::ScaleDown(item),
            _ => Self::NoAction(item),
        }
    }

    pub const fn from_direction(item: T, direction: ScaleDirection) -> Self {
        match direction {
            ScaleDirection::Up => Self::ScaleUp(item),
            ScaleDirection::Down => Self::ScaleDown(item),
            ScaleDirection::None => Self::NoAction(item),
        }
    }

    #[inline]
    #[allow(clippy::missing_const_for_fn)]
    pub fn item(&self) -> &T {
        match self {
            Self::ScaleUp(item) => item,
            Self::ScaleDown(item) => item,
            Self::NoAction(item) => item,
        }
    }

    pub const fn direction(&self) -> ScaleDirection {
        match self {
            Self::ScaleUp(_) => ScaleDirection::Up,
            Self::ScaleDown(_) => ScaleDirection::Down,
            Self::NoAction(_) => ScaleDirection::None,
        }
    }
}

impl<T: Correlation> Correlation for DecisionResult<T> {
    type Correlated = <T as Correlation>::Correlated;

    fn correlation(&self) -> &Id<Self::Correlated> {
        match self {
            Self::ScaleUp(item) => item.correlation(),
            Self::ScaleDown(item) => item.correlation(),
            Self::NoAction(item) => item.correlation(),
        }
    }
}

impl<T> From<DecisionResult<T>> for TelemetryValue
where
    T: Into<Self>,
{
    fn from(result: DecisionResult<T>) -> Self {
        match result {
            DecisionResult::ScaleUp(item) => Self::Table(
                maplit::hashmap! {
                    T_ITEM.to_string() => item.to_telemetry(),
                    T_SCALE_DECISION.to_string() => SCALE_UP.to_telemetry(),
                }
                .into(),
            ),
            DecisionResult::ScaleDown(item) => Self::Table(
                maplit::hashmap! {
                    T_ITEM.to_string() => item.to_telemetry(),
                    T_SCALE_DECISION.to_string() => SCALE_DOWN.to_telemetry(),
                }
                .into(),
            ),
            DecisionResult::NoAction(item) => Self::Table(
                maplit::hashmap! {
                    T_ITEM.to_string() => item.to_telemetry(),
                    T_SCALE_DECISION.to_string() => NO_ACTION.to_telemetry(),
                }
                .into(),
            ),
        }
    }
}

impl<T> TryFrom<TelemetryValue> for DecisionResult<T>
where
    T: TryFrom<TelemetryValue>,
    <T as TryFrom<TelemetryValue>>::Error: Into<TelemetryError>,
{
    type Error = DecisionError;

    fn try_from(value: TelemetryValue) -> Result<Self, Self::Error> {
        if let TelemetryValue::Table(ref table) = value {
            let item = table.get(T_ITEM).map_or_else(
                || Err(DecisionError::DataNotFound(T_ITEM.to_string())),
                |i| {
                    T::try_from(i.clone()).map_err(|err| {
                        let t_err: TelemetryError = err.into();
                        t_err.into()
                    })
                },
            )?;

            let decision = table.get(T_SCALE_DECISION).map_or_else(
                || Err(DecisionError::DataNotFound(T_SCALE_DECISION.to_string())),
                |d| String::try_from(d.clone()).map_err(|err| err.into()),
            )?;

            match decision.as_str() {
                SCALE_UP => Ok(Self::ScaleUp(item)),
                SCALE_DOWN => Ok(Self::ScaleDown(item)),
                rep => Err(DecisionError::Binding {
                    key: T_SCALE_DECISION.to_string(),
                    value: rep.to_string(),
                }),
            }
        } else {
            // todo resolves into DecisionError::Other. Improve precision?
            Err(proctor::error::TelemetryError::TypeError {
                expected: TelemetryType::Table,
                actual: Some(format!("{:?}", value)),
            }
            .into())
        }
    }
}
