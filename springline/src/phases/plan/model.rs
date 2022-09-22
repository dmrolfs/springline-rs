use std::fmt;

use oso::PolarClass;
use pretty_snowflake::Id;
use proctor::elements::Timestamp;
use proctor::Correlation;
use serde::{Deserialize, Serialize};

use crate::flink::MetricCatalog;
use crate::math;
use crate::phases::decision::DecisionResult;
use crate::phases::plan::{ScaleDirection, MINIMAL_JOB_PARALLELISM};
use crate::CorrelationId;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct ScaleParameters {
    pub calculated_job_parallelism: Option<u32>,
    pub nr_taskmanagers: u32,
    pub total_task_slots: Option<u32>,
    pub free_task_slots: Option<u32>,
    pub min_scaling_step: u32,
}

impl ScaleParameters {
    #[inline]
    pub fn task_slots_per_taskmanager(&self) -> f64 {
        self.total_task_slots
            .map(|task_slots| f64::from(task_slots) / f64::from(self.nr_taskmanagers))
            .unwrap_or(1_f64)
    }
}

#[derive(PolarClass, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScalePlan {
    pub correlation_id: CorrelationId,

    #[polar(attribute)]
    pub recv_timestamp: Timestamp,

    #[polar(attribute)]
    pub current_job_parallelism: u32,

    #[polar(attribute)]
    pub target_job_parallelism: u32,

    #[polar(attribute)]
    pub current_nr_taskmanagers: u32,

    #[polar(attribute)]
    pub target_nr_taskmanagers: u32,

    pub task_slots_per_taskmanager: f64,
}

impl fmt::Debug for ScalePlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScalePlan")
            .field("correlation_id", &self.correlation_id)
            .field("recv_timestamp", &format!("{}", self.recv_timestamp))
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

impl Correlation for ScalePlan {
    type Correlated = MetricCatalog;

    fn correlation(&self) -> &Id<Self::Correlated> {
        &self.correlation_id
    }
}

impl ScalePlan {
    #[tracing::instrument(level = "trace", name = "ScalePlan::new", skip())]
    pub fn new(
        decision: DecisionResult<MetricCatalog>, parameters: ScaleParameters,
    ) -> Option<Self> {
        let task_slots_per_taskmanager = parameters.task_slots_per_taskmanager();
        let taskmanagers_for_parallelism =
            |p: u32| Self::calculate_taskmanagers(p, task_slots_per_taskmanager);

        match decision.direction() {
            ScaleDirection::None => None,
            direction => {
                let current_job_parallelism = decision.item().health.job_nonsource_max_parallelism;
                let current_nr_taskmanagers = decision.item().cluster.nr_task_managers;

                let recv_timestamp = decision.item().recv_timestamp;
                let correlation_id = decision.item().correlation_id.clone();
                let (target_job_parallelism, target_nr_taskmanagers) = match direction {
                    ScaleDirection::Up => {
                        let min_target_parallelism =
                            current_job_parallelism.saturating_add(parameters.min_scaling_step);

                        let p = parameters
                            .calculated_job_parallelism
                            .map_or(min_target_parallelism, |calculated_p| {
                                u32::max(calculated_p, min_target_parallelism)
                            });
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
                        let max_target_parallelism =
                            current_job_parallelism.saturating_sub(parameters.min_scaling_step);

                        let p0 = parameters
                            .calculated_job_parallelism
                            .map_or(max_target_parallelism, |calculated_p| {
                                u32::min(calculated_p, max_target_parallelism)
                            });
                        let p = u32::max(MINIMAL_JOB_PARALLELISM, p0);
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
                    correlation_id,
                    recv_timestamp,
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
        let current = i64::from(self.current_job_parallelism);
        let target = i64::from(self.target_job_parallelism);
        target.saturating_sub(current)
    }

    pub fn nr_task_managers_difference(&self) -> i64 {
        let current = i64::from(self.current_nr_taskmanagers);
        let target = i64::from(self.target_nr_taskmanagers);
        target.saturating_sub(current)
    }

    pub fn parallelism_for_taskmanagers(&self, nr_taskmanagers: u32) -> u32 {
        math::try_f64_to_u32(self.task_slots_per_taskmanager * f64::from(nr_taskmanagers))
    }

    pub fn taskmanagers_for_parallelism(&self, parallelism: u32) -> u32 {
        Self::calculate_taskmanagers(parallelism, self.task_slots_per_taskmanager)
    }

    fn calculate_taskmanagers(parallelism: u32, task_slots_per_taskmanager: f64) -> u32 {
        math::try_f64_to_u32(f64::from(parallelism) / task_slots_per_taskmanager)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flink::{ClusterMetrics, FlowMetrics, JobHealthMetrics, MetricCatalog};
    use crate::phases::decision::DecisionResult;
    use crate::phases::plan::ScaleDirection;
    use pretty_assertions::assert_eq;

    use pretty_snowflake::{Id, Label, Labeling};
    use proctor::elements::Timestamp;
    use proptest::prelude::*;

    fn arb_direction_strategy() -> impl Strategy<Value = ScaleDirection> {
        prop_oneof![
            Just(ScaleDirection::Up),
            Just(ScaleDirection::Down),
            Just(ScaleDirection::None),
        ]
    }

    fn arb_calculated_parallelism_strategy(
        _direction: ScaleDirection, _current_parallelism: u32,
    ) -> impl Strategy<Value = Option<u32>> {
        prop::option::of(any::<u32>())
        // match direction {
        //     // ScaleDirection::Up if 200 <= current_parallelism => Just(Some(200)).boxed(),
        //     ScaleDirection::Up => ((current_parallelism + 1)..=250).prop_map(|p| Some(p)).boxed(),
        //     ScaleDirection::Down if current_parallelism < MINIMAL_JOB_PARALLELISM => Just(Some(MINIMAL_JOB_PARALLELISM)).boxed(),
        //     ScaleDirection::Down => (MINIMAL_JOB_PARALLELISM..current_parallelism).prop_map(|p| Some(p)).boxed(),
        //     ScaleDirection::None => Just(None).boxed(),
        // }
    }

    fn make_decision_result(
        direction: ScaleDirection, current_job_parallelism: u32, current_nr_task_managers: u32,
    ) -> DecisionResult<MetricCatalog> {
        let data = MetricCatalog {
            correlation_id: Id::direct(
                <MetricCatalog as Label>::labeler().label(),
                0,
                "<undefined>",
            ),
            recv_timestamp: Timestamp::now(),
            health: JobHealthMetrics {
                job_nonsource_max_parallelism: current_job_parallelism,
                ..JobHealthMetrics::default()
            },
            flow: FlowMetrics::default(),
            cluster: ClusterMetrics {
                nr_task_managers: current_nr_task_managers,
                ..ClusterMetrics::default()
            },
            custom: Default::default(),
        };

        DecisionResult::from_direction(data, direction)
    }

    proptest! {
        #[test]
        // fn test_scale_plan(
        //     (direction, current_nr_task_managers, current_job_parallelism, calculated_parallelism) in
        //         (arb_direction_strategy(), (0_u32..=100), (MINIMAL_JOB_PARALLELISM..=200)).prop_flat_map(|(d, cur_tm, cur_p)| {
        //         (Just(d), Just(cur_tm), Just(cur_p), arb_calculated_parallelism_strategy(d, cur_p))
        //     }),
        //     min_scaling_step in (0_u32..=10)
        // ) {
        fn test_scale_plan(
            (direction, current_nr_task_managers, current_job_parallelism, calculated_parallelism) in
                (arb_direction_strategy(), (MINIMAL_JOB_PARALLELISM..)).prop_flat_map(|(d, cur_p)| {
                (Just(d), (cur_p..), Just(cur_p), arb_calculated_parallelism_strategy(d, cur_p))
            }),
            min_scaling_step in any::<u32>()
        ) {
            let decision = make_decision_result(direction, current_job_parallelism, current_nr_task_managers);
            prop_assert_eq!(current_job_parallelism, decision.item().health.job_nonsource_max_parallelism);
            prop_assert_eq!(current_nr_task_managers, decision.item().cluster.nr_task_managers);

            let parameters = ScaleParameters {
                calculated_job_parallelism: calculated_parallelism,
                nr_taskmanagers: current_nr_task_managers,
                total_task_slots: Some(current_nr_task_managers),
                free_task_slots: Some(0),
                min_scaling_step,
            };
            if let Some(actual) = ScalePlan::new(decision.clone(), parameters) {
                prop_assert_eq!(&actual.correlation_id, &decision.item().correlation_id);
                prop_assert_eq!(actual.recv_timestamp, decision.item().recv_timestamp);
                prop_assert_eq!(actual.current_job_parallelism, current_job_parallelism);
                prop_assert_eq!(actual.current_nr_taskmanagers, current_nr_task_managers);

                let calculated_target = match direction {
                    ScaleDirection::Up => u32::max(current_job_parallelism, calculated_parallelism.unwrap_or(current_job_parallelism)),
                    ScaleDirection::Down => u32::min(current_job_parallelism, calculated_parallelism.unwrap_or(current_job_parallelism)),
                    ScaleDirection::None => current_job_parallelism,
                };

                let parallelism_diff = i64::from(calculated_target).saturating_sub(i64::from(current_job_parallelism));

                let up_min_adj_parallelism = u32::max(calculated_target, current_job_parallelism.saturating_add(min_scaling_step));
                let down_max_adj_parallelism = u32::max(
                    MINIMAL_JOB_PARALLELISM,
                    u32::min(calculated_target, current_job_parallelism.saturating_sub(min_scaling_step))
                );

                // gross check
                match direction {
                    ScaleDirection::Up => {
                        prop_assert!(actual.current_job_parallelism < actual.target_job_parallelism);
                        // prop_assert!(actual.current_nr_task_managers <= actual.target_nr_task_managers);
                    },

                    ScaleDirection::Down => {
                        prop_assert!(actual.target_job_parallelism < actual.current_job_parallelism);
                        // prop_assert!(actual.target_nr_task_managers <= actual.current_nr_task_managers);
                    },

                    ScaleDirection::None => {
                        prop_assert_eq!(actual.target_job_parallelism, actual.current_job_parallelism);
                        prop_assert_eq!(actual.target_nr_taskmanagers, actual.current_nr_taskmanagers);
                    },
                }

                // finer check
                let expected_tms = math::try_f64_to_u32(f64::from(actual.target_job_parallelism) / parameters.task_slots_per_taskmanager());
                prop_assert_eq!(actual.target_nr_taskmanagers, expected_tms);
                match direction {
                    ScaleDirection::Up if up_min_adj_parallelism < current_nr_task_managers && parallelism_diff.abs() <= min_scaling_step as i64 => {
                        prop_assert_eq!(actual.target_job_parallelism, u32::max(calculated_target, up_min_adj_parallelism));
                        // let expected_tms = math::try_f64_to_u32(f64::from(actual.target_job_parallelism) / parameters.task_slots_per_taskmanager());
                        // prop_assert_eq!(actual.target_nr_task_managers, expected_tms);
                        // prop_assert_eq!(actual.target_nr_task_managers, current_nr_task_managers);
                    },

                    ScaleDirection::Up if parallelism_diff.abs() <= min_scaling_step as i64 => {
                        prop_assert_eq!(actual.target_job_parallelism, u32::max(calculated_target, up_min_adj_parallelism));
                        // prop_assert_eq!(actual.target_nr_task_managers, actual.target_job_parallelism);
                    },

                    ScaleDirection::Up if up_min_adj_parallelism < current_nr_task_managers => {
                        prop_assert_eq!(actual.target_job_parallelism, u32::max(calculated_target, up_min_adj_parallelism));
                        // prop_assert_eq!(actual.target_nr_task_managers, current_nr_task_managers);
                    },

                    ScaleDirection::Up => {
                        prop_assert_eq!(actual.target_job_parallelism, u32::max(calculated_target, up_min_adj_parallelism));
                        // prop_assert_eq!(actual.target_nr_task_managers, actual.target_job_parallelism);
                    },

                    ScaleDirection::Down if current_nr_task_managers < down_max_adj_parallelism && parallelism_diff.abs() <= min_scaling_step as i64 => {
                        prop_assert_eq!(actual.target_job_parallelism, u32::max(MINIMAL_JOB_PARALLELISM, u32::min(calculated_target, down_max_adj_parallelism)));
                        // prop_assert_eq!(actual.target_nr_task_managers, current_nr_task_managers);
                    },

                    ScaleDirection::Down if parallelism_diff.abs() <= min_scaling_step as i64 => {
                        prop_assert_eq!(actual.target_job_parallelism, u32::max(MINIMAL_JOB_PARALLELISM, u32::min(calculated_target, down_max_adj_parallelism)));
                        // prop_assert_eq!(actual.target_nr_task_managers, actual.target_job_parallelism);
                    },

                    ScaleDirection::Down if current_nr_task_managers < down_max_adj_parallelism => {
                        prop_assert_eq!(actual.target_job_parallelism, u32::max(MINIMAL_JOB_PARALLELISM, u32::min(calculated_target, down_max_adj_parallelism)));
                        // prop_assert_eq!(actual.target_nr_task_managers, current_nr_task_managers);
                    },

                    ScaleDirection::Down => {
                        prop_assert_eq!(actual.target_job_parallelism, u32::max(MINIMAL_JOB_PARALLELISM, u32::min(calculated_target, down_max_adj_parallelism)));
                        // prop_assert_eq!(actual.target_nr_task_managers, actual.target_job_parallelism);
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
            nr_taskmanagers: current_nr_task_managers,
            total_task_slots: Some(current_nr_task_managers),
            free_task_slots: None,
            min_scaling_step,
        };
        if let Some(actual) = ScalePlan::new(decision.clone(), parameters) {
            assert_eq!(&actual.correlation_id, &decision.item().correlation_id);
            assert_eq!(actual.recv_timestamp, decision.item().recv_timestamp);
            assert_eq!(actual.current_job_parallelism, current_job_parallelism);
            assert_eq!(actual.current_nr_taskmanagers, current_nr_task_managers);

            let calculated_target = match direction {
                ScaleDirection::Up => u32::max(
                    current_job_parallelism,
                    calculated_parallelism.unwrap_or(current_job_parallelism),
                ),
                ScaleDirection::Down => u32::min(
                    current_job_parallelism,
                    calculated_parallelism.unwrap_or(current_job_parallelism),
                ),
                ScaleDirection::None => current_job_parallelism,
            };

            let parallelism_diff =
                i64::from(calculated_target).saturating_sub(i64::from(current_job_parallelism));

            let up_min_adj_parallelism = u32::max(
                calculated_target,
                current_job_parallelism.saturating_add(min_scaling_step),
            );
            let down_max_adj_parallelism = u32::max(
                MINIMAL_JOB_PARALLELISM,
                u32::min(
                    calculated_target,
                    current_job_parallelism.saturating_sub(min_scaling_step),
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
                    // assert!(actual.current_nr_task_managers <= actual.target_nr_task_managers);
                },

                ScaleDirection::Down => {
                    assert!(actual.target_job_parallelism < actual.current_job_parallelism);
                    // assert!(actual.target_nr_task_managers <= actual.current_nr_task_managers);
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

            let expected_tms = math::try_f64_to_u32(
                f64::from(actual.target_job_parallelism) / parameters.task_slots_per_taskmanager(),
            );
            assert_eq!(actual.target_nr_taskmanagers, expected_tms);
            match direction {
                ScaleDirection::Up
                    if up_min_adj_parallelism < current_nr_task_managers
                        && parallelism_diff.abs() <= min_scaling_step as i64 =>
                {
                    assert_eq!(
                        actual.target_job_parallelism,
                        u32::max(calculated_target, up_min_adj_parallelism)
                    );
                    // tracing::info!(?actual, %expected_tms);
                    // assert_eq!(actual.target_nr_task_managers, expected_tms);
                    // assert_eq!(actual.target_nr_task_managers, current_nr_task_managers);
                },

                ScaleDirection::Up if parallelism_diff.abs() <= min_scaling_step as i64 => {
                    assert_eq!(
                        actual.target_job_parallelism,
                        u32::max(calculated_target, up_min_adj_parallelism)
                    );
                    // assert_eq!(actual.target_nr_task_managers, expected_tms);
                    // assert_eq!(
                    //     actual.target_nr_task_managers,
                    //     actual.target_job_parallelism
                    // );
                },

                ScaleDirection::Up if up_min_adj_parallelism < current_nr_task_managers => {
                    assert_eq!(
                        actual.target_job_parallelism,
                        u32::max(calculated_target, up_min_adj_parallelism)
                    );
                    // assert_eq!(actual.target_nr_task_managers, current_nr_task_managers);
                },

                ScaleDirection::Up => {
                    assert_eq!(
                        actual.target_job_parallelism,
                        u32::max(calculated_target, up_min_adj_parallelism)
                    );
                    // assert_eq!(
                    //     actual.target_nr_task_managers,
                    //     actual.target_job_parallelism
                    // );
                },

                ScaleDirection::Down
                    if current_nr_task_managers < down_max_adj_parallelism
                        && parallelism_diff.abs() <= min_scaling_step as i64 =>
                {
                    assert_eq!(
                        actual.target_job_parallelism,
                        u32::max(
                            MINIMAL_JOB_PARALLELISM,
                            u32::min(calculated_target, down_max_adj_parallelism)
                        )
                    );
                    // assert_eq!(actual.target_nr_task_managers, current_nr_task_managers);
                },

                ScaleDirection::Down if parallelism_diff.abs() <= min_scaling_step as i64 => {
                    assert_eq!(
                        actual.target_job_parallelism,
                        u32::max(
                            MINIMAL_JOB_PARALLELISM,
                            u32::min(calculated_target, down_max_adj_parallelism)
                        )
                    );
                    // assert_eq!(
                    //     actual.target_nr_task_managers,
                    //     actual.target_job_parallelism
                    // );
                },

                ScaleDirection::Down if current_nr_task_managers < down_max_adj_parallelism => {
                    assert_eq!(
                        actual.target_job_parallelism,
                        u32::max(
                            MINIMAL_JOB_PARALLELISM,
                            u32::min(calculated_target, down_max_adj_parallelism)
                        )
                    );
                    // assert_eq!(actual.target_nr_task_managers, current_nr_task_managers);
                },

                ScaleDirection::Down => {
                    assert_eq!(
                        actual.target_job_parallelism,
                        u32::max(
                            MINIMAL_JOB_PARALLELISM,
                            u32::min(calculated_target, down_max_adj_parallelism)
                        )
                    );
                    // assert_eq!(
                    //     actual.target_nr_task_managers,
                    //     actual.target_job_parallelism
                    // );
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
}
