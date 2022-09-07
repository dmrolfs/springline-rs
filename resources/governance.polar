accept(plan, context, adjusted_target_parallelism, adjusted_target_nr_taskmanagers)
    if accept_scale_up(plan, context, adjusted_target_parallelism, adjusted_target_nr_taskmanagers)
    or accept_scale_down(plan, context, adjusted_target_parallelism, adjusted_target_nr_taskmanagers);

accept_scale_up(plan, context, adjusted_target_parallelism, adjusted_target_nr_taskmanagers)
    if not veto(plan, context)
    and scale_up(plan)
    and adjust_parallelism_step_up(plan, context, parallelism)
    and adjust_final_bounds(parallelism, context.min_parallelism, context.max_parallelism, adjusted_target_parallelism)
    and nr_taskmanagers = plan.taskmanagers_for_parallelism(adjusted_target_parallelism)
    and adjust_final_bounds(nr_taskmanagers, context.min_cluster_size, context.max_cluster_size, adjusted_target_nr_taskmanagers)
    and cut;

accept_scale_down(plan, context, adjusted_target_parallelism, adjusted_target_nr_taskmanagers)
    if not veto(plan, context)
    and scale_down(plan)
    and adjust_parallelism_step_down(plan, context, parallelism)
    and adjust_final_bounds(parallelism, context.min_parallelism, context.max_parallelism, adjusted_target_parallelism)
    and nr_taskmanagers = plan.taskmanagers_for_parallelism(adjusted_target_parallelism)
    and adjust_final_bounds(nr_taskmanagers, context.min_cluster_size, context.max_cluster_size, adjusted_target_nr_taskmanagers)
    and cut;

# adjust final bounds
adjust_final_bounds(target, min_bound, max_bound, adjusted_target)
    if max(target, min_bound, target_0)
    and min(target_0, max_bound, adjusted_target);


# parallelism steps
adjust_parallelism_step_up(plan, context, adjusted_target)
    if adjust_step_up(
        plan.current_job_parallelism,
        plan.target_job_parallelism,
        context.min_scaling_step,
        context.max_scaling_step,
        adjusted_target
    );

adjust_parallelism_step_down(plan, context, adjusted_target)
    if adjust_step_down(
        plan.current_job_parallelism,
        plan.target_job_parallelism,
        context.min_scaling_step,
        context.max_scaling_step,
        adjusted_target
    );

# nr task managers steps
adjust_nr_taskmanagers_step(plan, context, adjusted_target)
    if plan.current_nr_taskmanagers <= plan.target_nr_taskmanagers
    and adjust_step_up(
        plan.current_nr_taskmanagers,
        plan.target_nr_taskmanagers,
        context.min_scaling_step,
        context.max_scaling_step,
        adjusted_target
    );

adjust_nr_taskmanagers_step(plan, context, adjusted_target)
    if plan.target_nr_taskmanagers < plan.current_nr_taskmanagers
    and adjust_step_down(
        plan.current_nr_taskmanagers,
        plan.target_nr_taskmanagers,
        context.min_scaling_step,
        context.max_scaling_step,
        adjusted_target
    );



# adjust_nr_taskmanagers_step_up(plan, context, adjusted_target)
#     if adjust_step_up(
#         plan.current_nr_taskmanagers,
#         plan.target_nr_taskmanagers,
#         context.min_scaling_step,
#         context.max_scaling_step,
#         adjusted_target
#     );

# adjust_nr_taskmanagers_step_down(plan, context, adjusted_target)
#     if adjust_step_down(
#         plan.current_nr_taskmanagers,
#         plan.target_nr_taskmanagers,
#         context.min_scaling_step,
#         context.max_scaling_step,
#         adjusted_target
#     );

# steps
adjust_step_up(current, target, min_step, max_step, adjusted_target)
    if min_bound = current + min_step
    and max_bound = current + max_step
    and max(target, min_bound, target_0)
    and min(target_0, max_bound, adjusted_target);

adjust_step_down(current, target, min_step, max_step, adjusted_target)
    if max_bound = current - min_step
    and min_bound = current - max_step
    and max(target, min_bound, target_0)
    and min(target_0, max_bound, adjusted_target);


# identify direction
no_change(plan)
    if plan.current_job_parallelism == plan.target_job_parallelism;

scale_up(plan)
    if plan.current_job_parallelism < plan.target_job_parallelism;

scale_down(plan)
    if plan.target_job_parallelism < plan.current_job_parallelism;

# one form of veto
veto(plan, _context) if no_change(plan);
veto(plan, _context) if not scale_up(plan) and not scale_down(plan);

# basic ops
min(lhs, rhs, result) if lhs <= rhs and result = lhs;
min(lhs, rhs, result) if rhs < lhs and result = rhs;

max(lhs, rhs, result) if lhs <= rhs and result = rhs;
max(lhs, rhs, result) if rhs < lhs and result = lhs;
