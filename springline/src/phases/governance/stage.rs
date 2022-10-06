use super::GovernanceContext;
use crate::phases::governance::GovernanceMonitor;
use crate::phases::plan::{ScaleActionPlan, ScaleDirection};
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use proctor::elements::{Bindings, PolicyFilterEvent, QueryResult};
use proctor::error::{PolicyError, PortError};
use proctor::graph::stage::{Stage, WithMonitor};
use proctor::graph::{Inlet, Outlet, Port, SinkShape, SourceShape, PORT_CONTEXT, PORT_DATA};
use proctor::{AppData, ProctorResult};
use std::sync::Arc;
use tokio::sync::broadcast;

type Context = GovernanceContext;

#[derive(Debug)]
pub struct GovernanceStage<T> {
    name: String,
    context_inlet: Inlet<Context>,
    inlet: Inlet<T>,
    outlet: Outlet<T>,
    tx_monitor: broadcast::Sender<Arc<PolicyFilterEvent<T, Context>>>,
}

impl<T> GovernanceStage<T> {
    #[tracing::instrument(level = "trace", skip(name))]
    pub fn new(name: &str) -> Self {
        let context_inlet = Inlet::new(name, PORT_CONTEXT);
        let inlet = Inlet::new(name, PORT_DATA);
        let outlet = Outlet::new(name, PORT_DATA);
        let (tx_monitor, _) = broadcast::channel(num_cpus::get() * 2);
        let name = name.to_string();

        Self { name, context_inlet, inlet, outlet, tx_monitor }
    }

    #[inline]
    pub fn context_inlet(&self) -> Inlet<Context> {
        self.context_inlet.clone()
    }
}

impl<T> SinkShape for GovernanceStage<T> {
    type In = T;

    #[inline]
    fn inlet(&self) -> Inlet<Self::In> {
        self.inlet.clone()
    }
}

impl<T> SourceShape for GovernanceStage<T> {
    type Out = T;

    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

impl<T> WithMonitor for GovernanceStage<T> {
    type Receiver = GovernanceMonitor<T>;

    fn rx_monitor(&self) -> Self::Receiver {
        self.tx_monitor.subscribe()
    }
}

#[dyn_upcast]
#[async_trait]
impl<T> Stage for GovernanceStage<T>
where
    T: AppData + ScaleActionPlan + PartialEq,
{
    fn name(&self) -> &str {
        self.name.as_str()
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.do_check().await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", name = "run governance", skip(self))]
    async fn run(&mut self) -> ProctorResult<()> {
        self.do_run().await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn close(self: Box<Self>) -> ProctorResult<()> {
        self.do_close().await?;
        Ok(())
    }
}

impl<T> GovernanceStage<T>
where
    T: AppData + ScaleActionPlan + PartialEq,
{
    #[inline]
    async fn do_check(&self) -> Result<(), PortError> {
        self.inlet.check_attachment().await?;
        self.outlet.check_attachment().await?;
        self.context_inlet.check_attachment().await?;
        Ok(())
    }

    #[inline]
    async fn do_run(&mut self) -> Result<(), PolicyError> {
        let mut context = None;

        loop {
            let _timer = proctor::graph::stage::start_stage_eval_time(&self.name);

            tokio::select! {
                plan = self.inlet.recv() => match plan {
                    Some(p) => {
                        tracing::debug!(plan=?p, ?context, "{} handling next rescale plan...", self.name);

                        match context.as_ref() {
                            Some(ctx) => self.handle_plan(p, ctx,).await?,
                            None => self.handle_plan_before_context(p)?,
                        }
                    },
                    None => {
                        tracing::info!("{} Governance inlet, {:?} dropped - completing stage.", self.name, self.inlet);
                        break;
                    },
                },

                Some(incoming_context) = self.context_inlet.recv() => {
                    self.handle_context(&mut context, incoming_context).await?;
                    tracing::warn!("DMR: AFTER HANDLE CONTEXT: {context:?}");
                },

                else => {
                    tracing::info!("{} feed into governance depleted - breaking...", self.name);
                    break;
                },
            }
        }

        Ok(())
    }

    #[inline]
    async fn do_close(mut self: Box<Self>) -> Result<(), PortError> {
        tracing::trace!("closing governance ports");
        self.inlet.close().await;
        self.outlet.close().await;
        self.context_inlet.close().await;
        Ok(())
    }
}

impl<T> GovernanceStage<T>
where
    T: AppData + ScaleActionPlan + PartialEq,
{
    #[tracing::instrument(level="trace", name = "governance handle item", skip(self), fields(stage=%self.name))]
    async fn handle_plan(&mut self, plan: T, context: &Context) -> Result<(), PolicyError> {
        let event = if let Some(accepted_plan) = Self::assess(&plan, context) {
            tracing::info!(
                correlation=?accepted_plan.correlation(), ?accepted_plan,
                "plan passed {} policy review - sending via outlet", self.name
            );
            let policy_result = QueryResult::passed_without_bindings();
            self.outlet.send(accepted_plan.clone()).await?;
            PolicyFilterEvent::ItemPassed(accepted_plan, policy_result)
        } else {
            tracing::info!(correlation=?plan.correlation(), ?plan, "plan failed {} policy review - dropping.", self.name);
            let policy_result = QueryResult { passed: false, bindings: Bindings::default() };
            PolicyFilterEvent::ItemBlocked(plan, Some(policy_result))
        };
        self.publish_event(event)?;
        Ok(())
    }

    #[tracing::instrument(
        level = "trace",
        name = "governance handle plan before context set",
        skip(self)
    )]
    fn handle_plan_before_context(&self, plan: T) -> Result<(), PolicyError> {
        tracing::info!(correlation=?plan.correlation(), ?plan, "dropping plan received before governance context set.");
        self.publish_event(PolicyFilterEvent::ItemBlocked(plan, None))?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, context))]
    async fn handle_context(
        &self, context: &mut Option<Context>, recv_context: Context,
    ) -> Result<(), PolicyError> {
        *context = Some(recv_context);
        self.publish_event(PolicyFilterEvent::ContextChanged(context.clone()))?;
        Ok(())
    }

    #[tracing::instrument(level = "trace")]
    fn assess(proposed: &T, context: &Context) -> Option<T> {
        let correlation = proposed.correlation();
        let assessment_counter_plan = Self::accept_scale_up(proposed, context)
            .or_else(|| Self::accept_scale_down(proposed, context))
            .or_else(|| Self::accept_lateral_rescale(proposed, context));

        match assessment_counter_plan {
            None => {
                tracing::info!(
                    ?correlation,
                    ?proposed,
                    "governance rejected proposal - no workable counter proposal identified"
                );
                None
            },
            Some(counter_plan) if !Self::improves_utilization(&counter_plan) => {
                tracing::info!(
                    ?correlation,
                    ?counter_plan,
                    ?proposed,
                    "adjusted counter proposal does not improve cluster utilization - dropping."
                );

                None
            },
            Some(counter_plan) if proposed != &counter_plan => {
                tracing::info!(
                    ?correlation,
                    ?proposed,
                    ?counter_plan,
                    "governance accepted plan with adjustment."
                );
                Some(counter_plan)
            },
            plan => {
                tracing::info!(
                    ?correlation,
                    ?proposed,
                    ?plan,
                    "governance accepted plan without adjustment."
                );
                plan
            },
        }
    }

    fn improves_utilization(proposal: &T) -> bool {
        let same_parallelism =
            proposal.target_job_parallelism() == proposal.current_job_parallelism();
        let fewer_nr_task_managers = proposal.target_replicas() < proposal.current_replicas();
        let is_improvement = !same_parallelism || fewer_nr_task_managers; // karnaugh simplified expr
        tracing::debug!(
            ?proposal, %same_parallelism, %fewer_nr_task_managers,
            "governance check if accepted plan is an improvement: {is_improvement}"
        );
        is_improvement
    }

    #[inline]
    fn abs_diff(lhs: u32, rhs: u32) -> u32 {
        if lhs <= rhs {
            rhs.saturating_sub(lhs)
        } else {
            lhs.saturating_sub(rhs)
        }
    }

    #[inline]
    #[tracing::instrument(level = "trace", skip(from_current))]
    fn fit_target_into_constraints(
        current: u32, target: u32, min_bound: u32, max_bound: u32, context: &Context,
        from_current: impl Fn(u32) -> u32,
    ) -> u32 {
        let mut adjusted_target = target;
        tracing::debug!("DMR: AAA - adjusted_target={adjusted_target}");

        let min_step = context.min_scaling_step;
        let max_step = context.max_scaling_step;
        let diff = Self::abs_diff(current, adjusted_target);
        let effective_step = if diff < min_step {
            tracing::debug!(prior_step_adj_target=%adjusted_target, "DMR: BBB.1 - diff[{diff}] < min_step[{min_step}] => min_step");
            min_step
        } else if max_step < diff {
            tracing::debug!(prior_step_adj_target=%adjusted_target, "DMR: BBB.2 - max_step[{max_step}] < diff[{diff}] => max_step");
            max_step
        } else {
            tracing::debug!(prior_step_adj_target=%adjusted_target, "DMR: BBB.3 - => diff[{diff}]");
            diff
        };
        tracing::debug!(%effective_step, %adjusted_target, "DMR: CCC");
        adjusted_target = from_current(effective_step);

        tracing::debug!(
            "DMR: DDD - max(min_bound[{min_bound}, adjusted_target[{adjusted_target}] = {}",
            u32::max(min_bound, adjusted_target)
        );
        adjusted_target = u32::max(min_bound, adjusted_target);
        tracing::debug!(
            "DMR: EEE - min(adjusted_target[{adjusted_target}, max_bound[{max_bound}] = {}",
            u32::min(adjusted_target, max_bound)
        );
        adjusted_target = u32::min(adjusted_target, max_bound);
        tracing::debug!("DMR: FFF - final adjusted_target={adjusted_target}");
        adjusted_target
    }

    #[tracing::instrument(level = "trace")]
    fn accept_scale_up(plan: &T, context: &Context) -> Option<T> {
        if plan.direction() == ScaleDirection::Up {
            let mut proposed = plan.clone();

            let adjusted_target_parallelism = Self::fit_target_into_constraints(
                proposed.current_job_parallelism(),
                proposed.target_job_parallelism(),
                context.min_parallelism,
                context.max_parallelism,
                context,
                |step| proposed.current_job_parallelism().saturating_add(step),
            );
            proposed.set_target_job_parallelism(adjusted_target_parallelism);

            let adjusted_target_nr_taskmanagers = Self::fit_target_into_constraints(
                proposed.current_replicas(),
                proposed.target_replicas(),
                context.min_cluster_size,
                context.max_cluster_size,
                context,
                |step| proposed.current_replicas().saturating_add(step),
            );
            proposed.set_target_replicas(adjusted_target_nr_taskmanagers);

            if &proposed == plan {
                tracing::debug!(?proposed, "accept rescale up plan as specified.");
            } else {
                tracing::debug!(?proposed, "accept rescale up plan with proposed changes.");
            }
            Some(proposed)
        } else {
            tracing::debug!("plan not accepted for rescale up.");
            None
        }
    }

    #[tracing::instrument(level = "trace")]
    fn accept_scale_down(plan: &T, context: &Context) -> Option<T> {
        if plan.direction() == ScaleDirection::Down {
            let mut proposed = plan.clone();

            let adjusted_target_parallelism = Self::fit_target_into_constraints(
                proposed.current_job_parallelism(),
                proposed.target_job_parallelism(),
                context.min_parallelism,
                context.max_parallelism,
                context,
                |step| proposed.current_job_parallelism().saturating_sub(step),
            );
            proposed.set_target_job_parallelism(adjusted_target_parallelism);

            let adjusted_target_nr_taskmanagers = Self::fit_target_into_constraints(
                proposed.current_replicas(),
                proposed.target_replicas(),
                context.min_cluster_size,
                context.max_cluster_size,
                context,
                |step| proposed.current_replicas().saturating_sub(step),
            );
            proposed.set_target_replicas(adjusted_target_nr_taskmanagers);

            if &proposed == plan {
                tracing::debug!(?proposed, "accept rescale down plan as specified.");
            } else {
                tracing::debug!(?proposed, "accept rescale down plan with proposed changes.");
            }
            Some(proposed)
        } else {
            tracing::debug!("plan not accepted for rescale down.");
            None
        }
    }

    #[tracing::instrument(level = "trace")]
    fn accept_lateral_rescale(plan: &T, _context: &Context) -> Option<T> {
        if plan.direction() == ScaleDirection::None {
            tracing::debug!("accept lateral rescale plan.");
            Some(plan.clone())
        } else {
            tracing::debug!("plan not accepted for lateral rescale.");
            None
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn publish_event(&self, event: PolicyFilterEvent<T, Context>) -> Result<(), PolicyError> {
        let nr_notified = self
            .tx_monitor
            .send(Arc::new(event))
            .map_err(|err| PolicyError::Publish(err.into()))?;
        tracing::trace!("notifying {nr_notified} subscribers of governance event.");
        Ok(())
    }
}
