use crate::flink::Parallelism;
use crate::model::NrReplicas;
use crate::phases::decision::{DecisionDataT, DecisionResult};
use crate::phases::plan::ScaleDirection;
use crate::{math, Env};
use num_traits::{SaturatingAdd, SaturatingSub};
use oso::PolarClass;
use pretty_snowflake::Label;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Deref;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct ScaleParameters {
    pub calculated_job_parallelism: Option<Parallelism>,
    pub clipping_point: Option<Parallelism>,
    pub nr_task_managers: NrReplicas,
    pub total_task_slots: Option<u32>,
    pub free_task_slots: Option<u32>,
    pub min_scaling_step: u32,
}

impl ScaleParameters {
    #[inline]
    pub fn task_slots_per_taskmanager(&self) -> f64 {
        self.total_task_slots
            .map(|task_slots| f64::from(task_slots) / self.nr_task_managers.as_f64())
            .unwrap_or(1_f64)
    }
}

pub trait ScaleActionPlan {
    fn direction(&self) -> ScaleDirection;
    fn current_job_parallelism(&self) -> Parallelism;
    fn target_job_parallelism(&self) -> Parallelism;
    fn set_target_job_parallelism(&mut self, parallelism: Parallelism);
    fn current_replicas(&self) -> NrReplicas;
    fn target_replicas(&self) -> NrReplicas;
    fn set_target_replicas(&mut self, nr_replicas: NrReplicas);
    fn parallelism_factor(&self) -> Option<f64>;
    fn parallelism_for_replicas(&self, nr_replicas: NrReplicas) -> Option<Parallelism>;
    fn replicas_for_parallelism(&self, parallelism: Parallelism) -> Option<NrReplicas>;
}

impl<P> ScaleActionPlan for Env<P>
where
    P: Label + ScaleActionPlan,
{
    fn direction(&self) -> ScaleDirection {
        self.deref().direction()
    }

    fn current_job_parallelism(&self) -> Parallelism {
        self.deref().current_job_parallelism()
    }

    fn target_job_parallelism(&self) -> Parallelism {
        self.deref().target_job_parallelism()
    }

    fn set_target_job_parallelism(&mut self, parallelism: Parallelism) {
        self.as_mut().set_target_job_parallelism(parallelism)
    }

    fn current_replicas(&self) -> NrReplicas {
        self.deref().current_replicas()
    }

    fn target_replicas(&self) -> NrReplicas {
        self.deref().target_replicas()
    }

    fn set_target_replicas(&mut self, nr_replicas: NrReplicas) {
        self.as_mut().set_target_replicas(nr_replicas)
    }

    fn parallelism_factor(&self) -> Option<f64> {
        self.deref().parallelism_factor()
    }

    fn parallelism_for_replicas(&self, nr_replicas: NrReplicas) -> Option<Parallelism> {
        self.deref().parallelism_for_replicas(nr_replicas)
    }

    fn replicas_for_parallelism(&self, parallelism: Parallelism) -> Option<NrReplicas> {
        self.deref().replicas_for_parallelism(parallelism)
    }
}

#[derive(PolarClass, Label, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScalePlan {
    #[polar(attribute)]
    pub current_job_parallelism: Parallelism,

    #[polar(attribute)]
    pub target_job_parallelism: Parallelism,

    #[polar(attribute)]
    pub current_nr_taskmanagers: NrReplicas,

    #[polar(attribute)]
    pub target_nr_taskmanagers: NrReplicas,

    pub task_slots_per_taskmanager: f64,
}

impl fmt::Debug for ScalePlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScalePlan")
            .field(
                "job_parallelism_plan",
                &format!(
                    "{}->{}",
                    self.current_job_parallelism, self.target_job_parallelism
                ),
            )
            .field(
                "nr_taskmanagers_plan",
                &format!(
                    "{}->{}",
                    self.current_nr_taskmanagers, self.target_nr_taskmanagers
                ),
            )
            .field(
                "task_slots_per_taskmanager",
                &self.task_slots_per_taskmanager,
            )
            .finish()
    }
}

impl ScalePlan {
    #[tracing::instrument(level = "trace", name = "ScalePlan::new", skip())]
    pub fn new(
        decision: DecisionResult<DecisionDataT>, parameters: ScaleParameters,
    ) -> Option<Self> {
        let task_slots_per_taskmanager = parameters.task_slots_per_taskmanager();
        let taskmanagers_for_parallelism =
            |p: Parallelism| Self::calculate_taskmanagers(p, task_slots_per_taskmanager);

        match decision.direction() {
            ScaleDirection::None => None,
            direction => {
                let current_job_parallelism = decision.item().health.job_nonsource_max_parallelism;
                let current_nr_taskmanagers = decision.item().cluster.nr_task_managers;

                let clip_it = |p| apply_clipping(parameters.clipping_point, p);

                let (target_job_parallelism, target_nr_taskmanagers) = match direction {
                    ScaleDirection::Up => {
                        let min_target_parallelism = current_job_parallelism
                            .saturating_add(&Parallelism::new(parameters.min_scaling_step));

                        let p = parameters
                            .calculated_job_parallelism
                            .map_or(min_target_parallelism, |calculated_p| {
                                Parallelism::max(calculated_p, min_target_parallelism)
                            });
                        let p = clip_it(p);
                        let tms_for_parallelism = taskmanagers_for_parallelism(p);
                        let tms = tms_for_parallelism;
                        // let tms = u32::max(tms_for_parallelism, current_nr_taskmanagers);
                        tracing::debug!(
                            ?parameters,
                            %min_target_parallelism, %p, %tms_for_parallelism, %current_nr_taskmanagers, %tms,
                            "calculating UP ScalePlan"
                        );
                        (p, tms)
                    },
                    ScaleDirection::Down => {
                        let max_target_parallelism = current_job_parallelism
                            .saturating_sub(&Parallelism::new(parameters.min_scaling_step));

                        let p0 = parameters
                            .calculated_job_parallelism
                            .map_or(max_target_parallelism, |calculated_p| {
                                Parallelism::min(calculated_p, max_target_parallelism)
                            });
                        let p = Parallelism::max(Parallelism::MIN, p0);
                        let p = clip_it(p);
                        let tms_for_parallelism = taskmanagers_for_parallelism(p);
                        let tms = tms_for_parallelism;
                        // let tms = u32::min(tms_for_parallelism, current_nr_taskmanagers);
                        tracing::debug!(
                            ?parameters,
                            %max_target_parallelism, %p0, %p, %tms_for_parallelism, %current_nr_taskmanagers, %tms,
                            "calculating DOWN ScalePlan"
                        );
                        (p, tms)
                    },
                    ScaleDirection::None => (current_job_parallelism, current_nr_taskmanagers),
                };

                Some(Self {
                    current_job_parallelism,
                    target_job_parallelism,
                    current_nr_taskmanagers,
                    target_nr_taskmanagers,
                    task_slots_per_taskmanager,
                })
            },
        }
    }

    pub fn direction(&self) -> ScaleDirection {
        match (
            self.job_parallelism_difference(),
            self.nr_task_managers_difference(),
        ) {
            (p_diff, _) if 0 < p_diff => ScaleDirection::Up,
            (p_diff, _) if p_diff < 0 => ScaleDirection::Down,
            (_, tm_diff) if 0 < tm_diff => ScaleDirection::Up,
            (_, tm_diff) if tm_diff < 0 => ScaleDirection::Down,
            _ => ScaleDirection::None,
        }
    }

    pub fn job_parallelism_difference(&self) -> i64 {
        let current = i64::from(self.current_job_parallelism.as_u32());
        let target = i64::from(self.target_job_parallelism.as_u32());
        target.saturating_sub(current)
    }

    pub fn nr_task_managers_difference(&self) -> i64 {
        let current = i64::from(self.current_nr_taskmanagers.as_u32());
        let target = i64::from(self.target_nr_taskmanagers.as_u32());
        target.saturating_sub(current)
    }

    pub fn parallelism_for_taskmanagers(&self, nr_taskmanagers: NrReplicas) -> Parallelism {
        Parallelism::new(math::try_f64_to_u32(
            self.task_slots_per_taskmanager * nr_taskmanagers.as_f64(),
        ))
    }

    pub fn taskmanagers_for_parallelism(&self, parallelism: Parallelism) -> NrReplicas {
        Self::calculate_taskmanagers(parallelism, self.task_slots_per_taskmanager)
    }

    fn calculate_taskmanagers(
        parallelism: Parallelism, task_slots_per_taskmanager: f64,
    ) -> NrReplicas {
        NrReplicas::new(math::try_f64_to_u32(
            parallelism.as_f64() / task_slots_per_taskmanager,
        ))
    }
}

impl ScaleActionPlan for ScalePlan {
    fn direction(&self) -> ScaleDirection {
        self.direction()
    }

    fn current_job_parallelism(&self) -> Parallelism {
        self.current_job_parallelism
    }

    fn target_job_parallelism(&self) -> Parallelism {
        self.target_job_parallelism
    }

    fn set_target_job_parallelism(&mut self, parallelism: Parallelism) {
        self.target_job_parallelism = parallelism;
    }

    fn current_replicas(&self) -> NrReplicas {
        self.current_nr_taskmanagers
    }

    fn target_replicas(&self) -> NrReplicas {
        self.target_nr_taskmanagers
    }

    fn set_target_replicas(&mut self, nr_replicas: NrReplicas) {
        self.target_nr_taskmanagers = nr_replicas;
    }

    fn parallelism_factor(&self) -> Option<f64> {
        Some(self.task_slots_per_taskmanager)
    }

    fn parallelism_for_replicas(&self, nr_replicas: NrReplicas) -> Option<Parallelism> {
        Some(self.parallelism_for_taskmanagers(nr_replicas))
    }

    fn replicas_for_parallelism(&self, parallelism: Parallelism) -> Option<NrReplicas> {
        Some(self.taskmanagers_for_parallelism(parallelism))
    }
}

fn apply_clipping(clipping_point: Option<Parallelism>, parallelism: Parallelism) -> Parallelism {
    clipping_point
        .map(|cp| {
            let cp = Parallelism::max(Parallelism::MIN, cp - 1);
            Parallelism::min(cp, parallelism)
        })
        .unwrap_or(parallelism)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flink::{
        AppDataWindow, ClusterMetrics, FlowMetrics, JobHealthMetrics, MetricCatalog,
    };
    use crate::phases::decision::{DecisionOutcome, DecisionResult};
    use crate::phases::plan::ScaleDirection;
    use pretty_assertions::assert_eq;
    use std::cell::RefCell;
    use std::time::Duration;

    use crate::phases::plan::clipping::{ClippingHandling, TemporaryLimitCell};
    use pretty_snowflake::{Id, Label, Labeling};
    use proctor::elements::Timestamp;
    use proctor::{Correlation, MetaData, ReceivedAt};
    use proptest::prelude::*;

    fn arb_direction_strategy() -> impl Strategy<Value = ScaleDirection> {
        prop_oneof![
            Just(ScaleDirection::Up),
            Just(ScaleDirection::Down),
            Just(ScaleDirection::None),
        ]
    }

    #[allow(dead_code)]
    fn arb_clipping_handling() -> impl Strategy<Value = ClippingHandling> {
        prop_oneof![
            Just(ClippingHandling::Ignore),
            Just(ClippingHandling::PermanentLimit(None)),
            arb_duration().prop_map(|reset_timeout| {
                ClippingHandling::TemporaryLimit {
                    cell: RefCell::new(TemporaryLimitCell::new(reset_timeout)),
                }
            })
        ]
    }

    #[allow(dead_code)]
    fn arb_duration() -> impl Strategy<Value = Duration> {
        any::<u64>().prop_map(Duration::from_secs)
    }

    fn arb_calculated_parallelism_strategy(
        direction: ScaleDirection, current_parallelism: Parallelism,
    ) -> impl Strategy<Value = Option<Parallelism>> {
        // prop::option::of(any::<u32>())
        match direction {
            // ScaleDirection::Up if 200 <= current_parallelism => Just(Some(200)).boxed(),
            ScaleDirection::Up => (current_parallelism.as_u32()..=250)
                .prop_map(|p| Some(Parallelism::new(p)))
                .boxed(),
            ScaleDirection::Down if current_parallelism < Parallelism::MIN => {
                Just(Some(Parallelism::MIN)).boxed()
            },
            ScaleDirection::Down => (Parallelism::MIN.as_u32()..=current_parallelism.as_u32())
                .prop_map(|p| Some(Parallelism::new(p)))
                .boxed(),
            ScaleDirection::None => Just(None).boxed(),
        }
    }

    fn make_decision_result(
        direction: ScaleDirection, current_job_parallelism: Parallelism,
        current_nr_task_managers: NrReplicas,
    ) -> DecisionOutcome {
        let data = MetricCatalog {
            health: JobHealthMetrics {
                job_nonsource_max_parallelism: current_job_parallelism.into(),
                ..JobHealthMetrics::default()
            },
            flow: FlowMetrics::default(),
            cluster: ClusterMetrics {
                nr_task_managers: current_nr_task_managers.into(),
                ..ClusterMetrics::default()
            },
            custom: Default::default(),
        };

        let data = Env::from_parts(
            MetaData::from_parts(
                Id::direct(
                    <MetricCatalog as Label>::labeler().label(),
                    0,
                    "<undefined>",
                ),
                Timestamp::now(),
            ),
            data,
        );
        let data = data.flat_map(|d| AppDataWindow::from_size(d, 1, Duration::from_secs(120)));
        let data = data.map(|d| DecisionResult::from_direction(d, direction));
        data
    }

    fn test_scenario(
        direction: ScaleDirection, current_nr_task_managers: u32, current_job_parallelism: u32,
        calculated_parallelism: Option<u32>, clipping_point: Option<u32>, min_scaling_step: u32,
    ) {
        let current_nr_task_managers = NrReplicas::new(current_nr_task_managers);
        let current_job_parallelism = Parallelism::new(current_job_parallelism);
        let calculated_parallelism = calculated_parallelism.map(Parallelism::new);
        let clipping_point = clipping_point.map(Parallelism::new);

        let decision =
            make_decision_result(direction, current_job_parallelism, current_nr_task_managers);
        assert_eq!(
            current_job_parallelism,
            decision.item().health.job_nonsource_max_parallelism
        );
        assert_eq!(
            current_nr_task_managers,
            decision.item().cluster.nr_task_managers
        );

        let parameters = ScaleParameters {
            calculated_job_parallelism: calculated_parallelism,
            clipping_point,
            nr_task_managers: current_nr_task_managers,
            total_task_slots: Some(current_nr_task_managers.into()),
            free_task_slots: Some(0),
            min_scaling_step,
        };
        if let Some(actual) = ScalePlan::new(decision.clone().into_inner(), parameters) {
            assert_eq!(actual.current_job_parallelism, current_job_parallelism);
            assert_eq!(actual.current_nr_taskmanagers, current_nr_task_managers);

            let calculated_target = match direction {
                ScaleDirection::Up => Parallelism::max(
                    current_job_parallelism,
                    calculated_parallelism.unwrap_or(current_job_parallelism),
                ),
                ScaleDirection::Down => Parallelism::min(
                    current_job_parallelism,
                    calculated_parallelism.unwrap_or(current_job_parallelism),
                ),
                ScaleDirection::None => current_job_parallelism,
            };

            let parallelism_diff =
                i64::from(calculated_target.saturating_sub(&current_job_parallelism).as_u32());

            let clip_it = |p| apply_clipping(clipping_point, p);

            let up_min_adj_parallelism = Parallelism::max(
                calculated_target,
                current_job_parallelism.saturating_add(&Parallelism::new(min_scaling_step)),
            );
            let up_min_adj_parallelism = clip_it(up_min_adj_parallelism);

            let down_max_adj_parallelism = Parallelism::max(
                Parallelism::MIN,
                Parallelism::min(
                    calculated_target,
                    current_job_parallelism.saturating_sub(&Parallelism::new(min_scaling_step)),
                ),
            );
            let down_max_adj_parallelism = clip_it(down_max_adj_parallelism);

            // gross check
            match direction {
                ScaleDirection::Up => {
                    let current = clip_it(actual.current_job_parallelism);
                    let target = clip_it(actual.target_job_parallelism);
                    tracing::debug!(?actual, %current, %target, "checking actual plan for Up.");
                },

                ScaleDirection::Down => {
                    assert!(actual.target_job_parallelism <= actual.current_job_parallelism);
                },

                ScaleDirection::None => {
                    assert_eq!(
                        actual.target_job_parallelism,
                        actual.current_job_parallelism
                    );
                    assert_eq!(
                        actual.target_nr_taskmanagers,
                        actual.current_nr_taskmanagers
                    );
                },
            }

            // finer check
            let expected_tms = NrReplicas::new(math::try_f64_to_u32(
                actual.target_job_parallelism.as_f64() / parameters.task_slots_per_taskmanager(),
            ));
            assert_eq!(actual.target_nr_taskmanagers, expected_tms);
            match direction {
                ScaleDirection::Up
                    if up_min_adj_parallelism
                        < Parallelism::new(current_nr_task_managers.as_u32())
                        && parallelism_diff.abs() <= min_scaling_step as i64 =>
                {
                    let expected = Parallelism::max(calculated_target, up_min_adj_parallelism);
                    let expected = clip_it(expected);
                    tracing::info!(?actual, %expected, %calculated_target, %up_min_adj_parallelism, "AAA: UP");
                    assert_eq!(actual.target_job_parallelism, expected);
                },

                ScaleDirection::Up if parallelism_diff.abs() <= min_scaling_step as i64 => {
                    let expected = Parallelism::max(calculated_target, up_min_adj_parallelism);
                    let expected = clip_it(expected);
                    tracing::info!(?actual, expected_target=%expected,  "BBB: UP");
                    assert_eq!(actual.target_job_parallelism, expected);
                },

                ScaleDirection::Up
                    if up_min_adj_parallelism
                        < Parallelism::new(current_nr_task_managers.as_u32()) =>
                {
                    let expected = Parallelism::max(calculated_target, up_min_adj_parallelism);
                    let expected = clip_it(expected);
                    tracing::info!(?actual, expected_target=%expected,  "CCC: UP");
                    assert_eq!(actual.target_job_parallelism, expected)
                },

                ScaleDirection::Up => {
                    let expected = Parallelism::max(calculated_target, up_min_adj_parallelism);
                    let expected = clip_it(expected);
                    tracing::info!(?actual, expected_target=%expected,  "DDD: UP");
                    assert_eq!(actual.target_job_parallelism, expected);
                },

                ScaleDirection::Down
                    if Parallelism::new(current_nr_task_managers.as_u32())
                        < down_max_adj_parallelism
                        && parallelism_diff.abs() <= min_scaling_step as i64 =>
                {
                    let expected = Parallelism::max(
                        Parallelism::MIN,
                        Parallelism::min(calculated_target, down_max_adj_parallelism),
                    );
                    let expected = clip_it(expected);
                    tracing::info!(?actual, expected_target=%expected,  "EEE: DOWN");
                    assert_eq!(actual.target_job_parallelism, expected);
                    // prop_assert_eq!(actual.target_nr_task_managers, current_nr_task_managers);
                },

                ScaleDirection::Down if parallelism_diff.abs() <= min_scaling_step as i64 => {
                    let expected = Parallelism::max(
                        Parallelism::MIN,
                        Parallelism::min(calculated_target, down_max_adj_parallelism),
                    );
                    let expected = clip_it(expected);
                    tracing::info!(?actual, expected_target=%expected,  "FFF: DOWN");
                    assert_eq!(actual.target_job_parallelism, expected);
                },

                ScaleDirection::Down
                    if Parallelism::new(current_nr_task_managers.as_u32())
                        < down_max_adj_parallelism =>
                {
                    let expected = Parallelism::max(
                        Parallelism::MIN,
                        Parallelism::min(calculated_target, down_max_adj_parallelism),
                    );
                    let expected = clip_it(expected);
                    tracing::info!(?actual, expected_target=%expected,  "GGG: DOWN");
                    assert_eq!(actual.target_job_parallelism, expected);
                },

                ScaleDirection::Down => {
                    let expected = Parallelism::max(
                        Parallelism::MIN,
                        Parallelism::min(calculated_target, down_max_adj_parallelism),
                    );
                    let expected = clip_it(expected);
                    tracing::info!(?actual, expected_target=%expected,  "HHH: DOWN");
                    assert_eq!(actual.target_job_parallelism, expected);
                },

                ScaleDirection::None => {
                    assert!(
                        false,
                        "no scale should be created for no rescale, but got {:?}",
                        actual
                    );
                },
            }
        } else {
            assert_eq!(direction, ScaleDirection::None, "no plan for no direction");
        }
    }

    proptest! {
        #[test]
        fn test_scale_plan(
            (direction, current_nr_task_managers, current_job_parallelism, calculated_parallelism, clipping_point) in
                (arb_direction_strategy(), (Parallelism::MIN.as_u32()..100)).prop_flat_map(|(d, cur_p)| {
                (
                    Just(d),
                    (cur_p..200).prop_map(NrReplicas::new),
                    Just(Parallelism::new(cur_p)),
                    arb_calculated_parallelism_strategy(d, Parallelism::new(cur_p)),
                    prop::option::of((1_u32..100).prop_map(Parallelism::new))
                )
            }),
            min_scaling_step in (1_u32..)
        ) {
            let decision = make_decision_result(direction, current_job_parallelism, current_nr_task_managers);
            prop_assert_eq!(current_job_parallelism, decision.item().health.job_nonsource_max_parallelism);
            prop_assert_eq!(current_nr_task_managers, decision.item().cluster.nr_task_managers);

            once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
            let main_span = tracing::info_span!("test_scale_plan");
            let _main_span_guard = main_span.enter();

            let parameters = ScaleParameters {
                calculated_job_parallelism: calculated_parallelism,
                clipping_point,
                nr_task_managers: current_nr_task_managers,
                total_task_slots: Some(current_nr_task_managers.as_u32()),
                free_task_slots: Some(0),
                min_scaling_step,
            };
            if let Some(actual) = decision.clone().map(|d| ScalePlan::new(d, parameters)).transpose() {
                prop_assert_eq!(&actual.correlation().num(), &decision.correlation().num());
                prop_assert_eq!(&actual.correlation().pretty(), &decision.correlation().pretty());
                prop_assert_eq!(actual.recv_timestamp(), decision.recv_timestamp());
                prop_assert_eq!(actual.current_job_parallelism, current_job_parallelism);
                prop_assert_eq!(actual.current_nr_taskmanagers, current_nr_task_managers);

                let calculated_target = match direction {
                    ScaleDirection::Up => Parallelism::max(current_job_parallelism, calculated_parallelism.unwrap_or(current_job_parallelism)),
                    ScaleDirection::Down => Parallelism::min(current_job_parallelism, calculated_parallelism.unwrap_or(current_job_parallelism)),
                    ScaleDirection::None => current_job_parallelism,
                };

                let parallelism_diff = i64::from(calculated_target.saturating_sub(&current_job_parallelism).as_u32());

                let clip_it = |p| { apply_clipping(clipping_point, p) };

                let up_min_adj_parallelism = Parallelism::max(calculated_target, current_job_parallelism.saturating_add(&Parallelism::new(min_scaling_step)));
                let up_min_adj_parallelism = clip_it(up_min_adj_parallelism);

                let down_max_adj_parallelism = Parallelism::max(
                    Parallelism::MIN,
                    Parallelism::min(calculated_target, current_job_parallelism.saturating_sub(&Parallelism::new(min_scaling_step)))
                );
                let down_max_adj_parallelism = clip_it(down_max_adj_parallelism);

                // gross check
                match direction {
                    ScaleDirection::Up => {
                        let current = clip_it(actual.current_job_parallelism);
                        let target = clip_it(actual.target_job_parallelism);
                        tracing::debug!(?actual, %current, %target, "checking actual plan for Up.");
                    },

                    ScaleDirection::Down => {
                        prop_assert!(actual.target_job_parallelism <= actual.current_job_parallelism);
                    },

                    ScaleDirection::None => {
                        prop_assert_eq!(actual.target_job_parallelism, actual.current_job_parallelism);
                        prop_assert_eq!(actual.target_nr_taskmanagers, actual.current_nr_taskmanagers);
                    },
                }

                // finer check
                let expected_tms = NrReplicas::new(
                    math::try_f64_to_u32(actual.target_job_parallelism.as_f64() / parameters.task_slots_per_taskmanager())
                );
                prop_assert_eq!(actual.target_nr_taskmanagers, expected_tms);
                match direction {
                    ScaleDirection::Up
                    if up_min_adj_parallelism < Parallelism::new(current_nr_task_managers.as_u32())
                    && parallelism_diff.abs() <= min_scaling_step as i64 => {
                        let expected = Parallelism::max(calculated_target, up_min_adj_parallelism);
                        let expected = clip_it(expected);
                        tracing::info!(?actual, %expected, %calculated_target, %up_min_adj_parallelism, "AAA: UP");
                        prop_assert_eq!(actual.target_job_parallelism, expected);
                    },

                    ScaleDirection::Up if parallelism_diff.abs() <= min_scaling_step as i64 => {
                        let expected = Parallelism::max(calculated_target, up_min_adj_parallelism);
                        let expected = clip_it(expected);
                        tracing::info!(?actual, expected_target=%expected,  "BBB: UP");
                        prop_assert_eq!(actual.target_job_parallelism, expected);
                    },

                    ScaleDirection::Up if up_min_adj_parallelism < Parallelism::new(current_nr_task_managers.as_u32()) => {
                        let expected = Parallelism::max(calculated_target, up_min_adj_parallelism);
                        let expected = clip_it(expected);
                        tracing::info!(?actual, expected_target=%expected,  "CCC: UP");
                        prop_assert_eq!(actual.target_job_parallelism, expected);
                    },

                    ScaleDirection::Up => {
                        let expected = Parallelism::max(calculated_target, up_min_adj_parallelism);
                        let expected = clip_it(expected);
                        tracing::info!(?actual, expected_target=%expected,  "DDD: UP");
                        prop_assert_eq!(actual.target_job_parallelism, expected);
                    },

                    ScaleDirection::Down
                    if Parallelism::new(current_nr_task_managers.as_u32()) < down_max_adj_parallelism
                    && parallelism_diff.abs() <= min_scaling_step as i64 => {
                        let expected = Parallelism::max(Parallelism::MIN, Parallelism::min(calculated_target, down_max_adj_parallelism));
                        let expected = clip_it(expected);
                        tracing::info!(?actual, expected_target=%expected,  "EEE: DOWN");
                        prop_assert_eq!(actual.target_job_parallelism, expected);
                    },

                    ScaleDirection::Down if parallelism_diff.abs() <= min_scaling_step as i64 => {
                        let expected = Parallelism::max(Parallelism::MIN, Parallelism::min(calculated_target, down_max_adj_parallelism));
                        let expected = clip_it(expected);
                        tracing::info!(?actual, expected_target=%expected,  "FFF: DOWN");
                        prop_assert_eq!(actual.target_job_parallelism, expected);
                    },

                    ScaleDirection::Down if Parallelism::new(current_nr_task_managers.as_u32()) < down_max_adj_parallelism => {
                        let expected = Parallelism::max(Parallelism::MIN, Parallelism::min(calculated_target, down_max_adj_parallelism));
                        let expected = clip_it(expected);
                        tracing::info!(?actual, expected_target=%expected,  "GGG: DOWN");
                        prop_assert_eq!(actual.target_job_parallelism, expected);
                    },

                    ScaleDirection::Down => {
                        let expected = Parallelism::max(Parallelism::MIN, Parallelism::min(calculated_target, down_max_adj_parallelism));
                        let expected = clip_it(expected);
                        tracing::info!(?actual, expected_target=%expected,  "HHH: DOWN");
                        prop_assert_eq!(actual.target_job_parallelism, expected);
                    },

                    ScaleDirection::None => {
                        prop_assert!(false, "no scale should be created for no rescale, but got {:?}", actual);
                    },
                }
            } else {
                prop_assert_eq!(direction, ScaleDirection::None, "no plan for no direction");
            }
        }
    }

    #[test]
    fn test_specific_scale_plan() {
        let (direction, current_nr_task_managers, current_job_parallelism, calculated_parallelism) =
            (ScaleDirection::Up, 501617634, 99, Some(467679096));
        let min_scaling_step = 501120066;

        let current_nr_task_managers = NrReplicas::new(current_nr_task_managers);
        let current_job_parallelism = Parallelism::new(current_job_parallelism);
        let calculated_parallelism = calculated_parallelism.map(Parallelism::new);

        let decision =
            make_decision_result(direction, current_job_parallelism, current_nr_task_managers);

        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!(
            "test_specific_scale_plan",
            %direction, %current_nr_task_managers, %current_job_parallelism, ?calculated_parallelism, %min_scaling_step,
        );
        let _main_span_guard = main_span.enter();

        assert_eq!(
            current_job_parallelism,
            decision.item().health.job_nonsource_max_parallelism
        );
        assert_eq!(
            current_nr_task_managers,
            decision.item().cluster.nr_task_managers
        );

        let parameters = ScaleParameters {
            calculated_job_parallelism: calculated_parallelism,
            clipping_point: None,
            nr_task_managers: current_nr_task_managers,
            total_task_slots: Some(current_nr_task_managers.into()),
            free_task_slots: None,
            min_scaling_step,
        };
        if let Some(actual) = decision.clone().map(|d| ScalePlan::new(d, parameters)).transpose() {
            assert_eq!(&actual.correlation().num(), &decision.correlation().num());
            assert_eq!(
                &actual.correlation().pretty(),
                &decision.correlation().pretty()
            );
            assert_eq!(actual.recv_timestamp(), decision.recv_timestamp());
            assert_eq!(actual.current_job_parallelism, current_job_parallelism);
            assert_eq!(actual.current_nr_taskmanagers, current_nr_task_managers);

            let calculated_target = match direction {
                ScaleDirection::Up => Parallelism::max(
                    current_job_parallelism,
                    calculated_parallelism.unwrap_or(current_job_parallelism),
                ),
                ScaleDirection::Down => Parallelism::min(
                    current_job_parallelism,
                    calculated_parallelism.unwrap_or(current_job_parallelism),
                ),
                ScaleDirection::None => current_job_parallelism,
            };

            let parallelism_diff =
                i64::from(calculated_target.saturating_sub(&current_job_parallelism).as_u32());

            let up_min_adj_parallelism = Parallelism::max(
                calculated_target,
                current_job_parallelism.saturating_add(&Parallelism::new(min_scaling_step)),
            );
            let down_max_adj_parallelism = Parallelism::max(
                Parallelism::MIN,
                Parallelism::min(
                    calculated_target,
                    current_job_parallelism.saturating_sub(&Parallelism::new(min_scaling_step)),
                ),
            );

            tracing::info!(
                ?actual, %calculated_target, %parallelism_diff, %up_min_adj_parallelism, %down_max_adj_parallelism,
                "AAA"
            );

            // gross check
            match direction {
                ScaleDirection::Up => {
                    assert!(actual.current_job_parallelism < actual.target_job_parallelism);
                },

                ScaleDirection::Down => {
                    assert!(actual.target_job_parallelism < actual.current_job_parallelism);
                },

                ScaleDirection::None => {
                    assert_eq!(
                        actual.target_job_parallelism,
                        actual.current_job_parallelism
                    );
                    assert_eq!(
                        actual.target_nr_taskmanagers,
                        actual.current_nr_taskmanagers
                    );
                },
            }

            let expected_tms = NrReplicas::new(math::try_f64_to_u32(
                actual.target_job_parallelism.as_f64() / parameters.task_slots_per_taskmanager(),
            ));
            assert_eq!(actual.target_nr_taskmanagers, expected_tms);
            match direction {
                ScaleDirection::Up
                    if up_min_adj_parallelism
                        < Parallelism::new(current_nr_task_managers.as_u32())
                        && parallelism_diff.abs() <= min_scaling_step as i64 =>
                {
                    assert_eq!(
                        actual.target_job_parallelism,
                        Parallelism::max(calculated_target, up_min_adj_parallelism)
                    );
                },

                ScaleDirection::Up if parallelism_diff.abs() <= min_scaling_step as i64 => {
                    assert_eq!(
                        actual.target_job_parallelism,
                        Parallelism::max(calculated_target, up_min_adj_parallelism)
                    );
                },

                ScaleDirection::Up
                    if up_min_adj_parallelism
                        < Parallelism::new(current_nr_task_managers.as_u32()) =>
                {
                    assert_eq!(
                        actual.target_job_parallelism,
                        Parallelism::max(calculated_target, up_min_adj_parallelism)
                    );
                },

                ScaleDirection::Up => {
                    assert_eq!(
                        actual.target_job_parallelism,
                        Parallelism::max(calculated_target, up_min_adj_parallelism)
                    );
                },

                ScaleDirection::Down
                    if Parallelism::new(current_nr_task_managers.as_u32())
                        < down_max_adj_parallelism
                        && parallelism_diff.abs() <= min_scaling_step as i64 =>
                {
                    assert_eq!(
                        actual.target_job_parallelism,
                        Parallelism::max(
                            Parallelism::MIN,
                            Parallelism::min(calculated_target, down_max_adj_parallelism)
                        )
                    );
                },

                ScaleDirection::Down if parallelism_diff.abs() <= min_scaling_step as i64 => {
                    assert_eq!(
                        actual.target_job_parallelism,
                        Parallelism::max(
                            Parallelism::MIN,
                            Parallelism::min(calculated_target, down_max_adj_parallelism)
                        )
                    );
                },

                ScaleDirection::Down
                    if Parallelism::new(current_nr_task_managers.as_u32())
                        < down_max_adj_parallelism =>
                {
                    assert_eq!(
                        actual.target_job_parallelism,
                        Parallelism::max(
                            Parallelism::MIN,
                            Parallelism::min(calculated_target, down_max_adj_parallelism)
                        )
                    );
                },

                ScaleDirection::Down => {
                    assert_eq!(
                        actual.target_job_parallelism,
                        Parallelism::max(
                            Parallelism::MIN,
                            Parallelism::min(calculated_target, down_max_adj_parallelism)
                        )
                    );
                },

                ScaleDirection::None => {
                    assert!(
                        false,
                        "no scale should be created for no rescale, but got {:?}",
                        actual
                    );
                },
            }
        } else {
            assert_eq!(direction, ScaleDirection::None, "no plan for no direction");
        }
    }

    #[test]
    fn test_specific_scale_plan_e80ba967a8() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_specific_scale_plan_e80ba967a8");
        let _main_span_guard = main_span.enter();

        // cc f473f9d81c199a9a900c162f43918834f1f7b7572ebbf152c3cc41e80ba967a8 # shrinks to (direction, current_nr_task_managers, current_job_parallelism, calculated_parallelism, clipping_point) = (Up, 208890, 208890, None, Some(148159224)), min_scaling_step = 147950320

        let (
            direction,
            current_nr_task_managers,
            current_job_parallelism,
            calculated_parallelism,
            clipping_point,
        ) = (ScaleDirection::Up, 208890, 208890, None, Some(148159224));
        let min_scaling_step = 147950320;

        test_scenario(
            direction,
            current_nr_task_managers,
            current_job_parallelism,
            calculated_parallelism,
            clipping_point,
            min_scaling_step,
        )
    }

    #[test]
    fn test_specific_scale_plan_95466d12be6() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_specific_scale_plan_95466d12be6");
        let _main_span_guard = main_span.enter();

        // cc 69bb6a5f56068bcd599a7602d9d449fc36a9c0cdbb7dc6a9ebb2e95466d12be6 # shrinks to (direction, current_nr_task_managers, current_job_parallelism, calculated_parallelism, clipping_point) = (Up, 1, 1, Some(1), None), min_scaling_step = 1

        let (
            direction,
            current_nr_task_managers,
            current_job_parallelism,
            calculated_parallelism,
            clipping_point,
        ) = (ScaleDirection::Up, 1, 1, Some(1), None);
        let min_scaling_step = 1;

        test_scenario(
            direction,
            current_nr_task_managers,
            current_job_parallelism,
            calculated_parallelism,
            clipping_point,
            min_scaling_step,
        )
    }

    #[test]
    fn test_specific_scale_plan_wip() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_specific_scale_plan_wip");
        let _main_span_guard = main_span.enter();

        // cc b3d2f92ef005024f676107e8cbe251c9036fcfc1ebcf5abcb92a1f1464c58bc4 # shrinks to (direction, current_nr_task_managers, current_job_parallelism, calculated_parallelism, clipping_point) = (Up, 39, 1, Some(39), Some(1)), min_scaling_step = 1

        let (
            direction,
            current_nr_task_managers,
            current_job_parallelism,
            calculated_parallelism,
            clipping_point,
        ) = (ScaleDirection::Up, 39, 1, Some(39), Some(1));
        let min_scaling_step = 1;

        test_scenario(
            direction,
            current_nr_task_managers,
            current_job_parallelism,
            calculated_parallelism,
            clipping_point,
            min_scaling_step,
        )
    }
}
