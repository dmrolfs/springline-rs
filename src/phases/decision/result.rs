use std::cmp::Ordering;
use std::convert::TryFrom;
use std::fmt::{self, Debug};

use itertools::Itertools;

use proctor::elements::{PolicyOutcome, TelemetryValue, ToTelemetry};
use proctor::error::{DecisionError, TelemetryError, TypeExpectation};
use proctor::graph::stage::{self, ThroughStage};
use proctor::{AppData, ProctorContext};

pub const DECISION_BINDING: &'static str = "direction";
pub const SCALE_UP: &'static str = "up";
pub const SCALE_DOWN: &'static str = "down";
pub const NO_ACTION: &'static str = "no action";

pub fn make_decision_transform<T, C, S>(
    name: S,
) -> impl ThroughStage<PolicyOutcome<T, C>, DecisionResult<T>>
where
    T: AppData + PartialEq,
    C: ProctorContext,
    S: Into<String>,
{
    stage::Map::new(name, move |outcome: PolicyOutcome<T, C>| {
        let transform_span = tracing::info_span!(
            "distill policy outcome into action",
            item=?outcome.item, policy_results=?outcome.policy_results
        );
        let _transform_span_guard = transform_span.enter();

        if outcome.passed() {
            outcome
                .policy_results
                .binding(DECISION_BINDING)
                .map(|directions: Vec<String>| {
                    let mut grouped: Vec<(String, usize)> = vec![];
                    for (direction, votes) in &directions.into_iter().group_by(|d| d.clone()) {
                        grouped.push((direction, votes.count()));
                    }
                    grouped.sort_by(|lhs, rhs| {
                        let cmp = lhs.1.cmp(&rhs.1);
                        if cmp != Ordering::Equal {
                            cmp
                        } else {
                            if lhs.0 == SCALE_UP {
                                Ordering::Less
                            } else {
                                Ordering::Greater
                            }
                        }
                    });

                    grouped
                        .first()
                        .map(|(d, _)| match d.as_str() {
                            SCALE_UP => DecisionResult::ScaleUp(outcome.item.clone()),
                            SCALE_DOWN => DecisionResult::ScaleDown(outcome.item.clone()),
                            direction => {
                                tracing::warn!(%direction, "unknown direction determined by policy - NoAction");
                                DecisionResult::NoAction(outcome.item.clone())
                            },
                        })
                        .unwrap_or_else(|| {
                            tracing::warn!("no direction determined by policy - NoAction");
                            DecisionResult::NoAction(outcome.item.clone())
                        })
                })
                .unwrap_or_else(|err| {
                    tracing::error!(error=?err, "policy failed to assess direction - NoAction.");
                    DecisionResult::NoAction(outcome.item)
                })
        } else {
            tracing::debug!("item did not pass context policy review.");
            DecisionResult::NoAction(outcome.item)
        }
    })
}

const T_ITEM: &'static str = "item";
const T_SCALE_DECISION: &'static str = "scale_decision";

#[derive(Debug, Clone, PartialEq)]
pub enum DecisionResult<T>
where
    T: Debug + Clone + PartialEq,
{
    ScaleUp(T),
    ScaleDown(T),
    NoAction(T),
}

impl<T> DecisionResult<T>
where
    T: Debug + Clone + PartialEq,
{
    pub fn new(item: T, decision_rep: &str) -> Self {
        match decision_rep {
            SCALE_UP => DecisionResult::ScaleUp(item),
            SCALE_DOWN => DecisionResult::ScaleDown(item),
            _ => DecisionResult::NoAction(item),
        }
    }

    #[inline]
    pub fn item(&self) -> &T {
        match self {
            DecisionResult::ScaleUp(item) => item,
            DecisionResult::ScaleDown(item) => item,
            DecisionResult::NoAction(item) => item,
        }
    }
}

impl<T> fmt::Display for DecisionResult<T>
where
    T: Debug + Clone + PartialEq,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            DecisionResult::ScaleUp(_) => "scale_up",
            DecisionResult::ScaleDown(_) => "scale_down",
            DecisionResult::NoAction(_) => "no_action",
        };

        write!(f, "{}", label)
    }
}

impl<T> Into<TelemetryValue> for DecisionResult<T>
where
    T: Into<TelemetryValue> + Debug + Clone + PartialEq,
{
    fn into(self) -> TelemetryValue {
        match self {
            DecisionResult::ScaleUp(item) => TelemetryValue::Table(maplit::hashmap! {
                T_ITEM.to_string() => item.to_telemetry(),
                T_SCALE_DECISION.to_string() => SCALE_UP.to_telemetry(),
            }),
            DecisionResult::ScaleDown(item) => TelemetryValue::Table(maplit::hashmap! {
                T_ITEM.to_string() => item.to_telemetry(),
                T_SCALE_DECISION.to_string() => SCALE_DOWN.to_telemetry(),
            }),
            DecisionResult::NoAction(item) => TelemetryValue::Table(maplit::hashmap! {
                T_ITEM.to_string() => item.to_telemetry(),
                T_SCALE_DECISION.to_string() => NO_ACTION.to_telemetry(),
            }),
        }
    }
}

impl<T> TryFrom<TelemetryValue> for DecisionResult<T>
where
    T: TryFrom<TelemetryValue> + Debug + Clone + PartialEq,
    <T as TryFrom<TelemetryValue>>::Error: Into<TelemetryError>,
{
    type Error = DecisionError;

    fn try_from(value: TelemetryValue) -> Result<Self, Self::Error> {
        if let TelemetryValue::Table(ref table) = value {
            let item = if let Some(i) = table.get(T_ITEM) {
                T::try_from(i.clone()).map_err(|err| {
                    let t_err: TelemetryError = err.into();
                    t_err.into()
                })
            } else {
                Err(DecisionError::DataNotFound(T_ITEM.to_string()))
            }?;

            let decision = if let Some(d) = table.get(T_SCALE_DECISION) {
                String::try_from(d.clone()).map_err(|err| err.into())
            } else {
                Err(DecisionError::DataNotFound(T_SCALE_DECISION.to_string()))
            }?;

            match decision.as_str() {
                SCALE_UP => Ok(DecisionResult::ScaleUp(item)),
                SCALE_DOWN => Ok(DecisionResult::ScaleDown(item)),
                rep => Err(DecisionError::ParseError(rep.to_string())),
            }
        } else if let TelemetryValue::Unit = value {
            Err(proctor::error::TelemetryError::TypeError {
                expected: format!("telemetry {} value", TypeExpectation::Table),
                actual: Some(format!("{:?}", value)),
            }
            .into())
        } else {
            // todo resolves into DecisionError::Other. Improve precision?
            Err(proctor::error::TelemetryError::TypeError {
                expected: format!("telemetry {} value", TypeExpectation::Table),
                actual: Some(format!("{:?}", value)),
            }
            .into())
        }
    }
}
