accept(plan, context, adjusted_target)
    if accept_scale_up(plan, context, adjusted_target)
    or accept_scale_down(plan, context, adjusted_target);


accept_scale_up(plan, context, adjusted_target)
    if check_scale_up(plan, context, adjusted)
    and context.max_cluster_size < adjusted
    and adjusted_target = context.max_cluster_size
    and cut;

accept_scale_up(plan, context, adjusted_target)
    if check_scale_up(plan, context, adjusted_target)
    and cut;

check_scale_up(plan, context, adjusted_target)
    if not veto(plan, context)
    and accept_step_up(plan, context, adjusted_target);


accept_scale_down(plan, context, adjusted_target)
    if check_scale_down(plan, context, adjusted)
    and adjusted < context.min_cluster_size
    and adjusted_target = context.min_cluster_size
    and cut;

accept_scale_down(plan, context, adjusted_target)
    if check_scale_down(plan, context, adjusted_target)
    and cut;

check_scale_down(plan, context, adjusted_target)
    if not veto(plan, context)
    and accept_step_down(plan, context, adjusted_target);


accept_step_up(plan, context, adjusted_target)
    if scale_up(plan)
    and (plan.target_nr_task_managers - plan.current_nr_task_managers) <= context.max_scaling_step
    and adjusted_target = plan.target_nr_task_managers;

accept_step_up(plan, context, adjusted_target)
    if scale_up(plan)
    and context.max_scaling_step < (plan.target_nr_task_managers - plan.current_nr_task_managers)
    and adjusted_target = plan.current_nr_task_managers + context.max_scaling_step;


accept_step_down(plan, context, adjusted_target)
    if scale_down(plan)
    and (plan.current_nr_task_managers - plan.target_nr_task_managers) <= context.max_scaling_step
    and adjusted_target = plan.target_nr_task_managers;

accept_step_down(plan, context, adjusted_target)
    if scale_down(plan)
    and context.max_scaling_step < (plan.current_nr_task_managers - plan.target_nr_task_managers)
    and adjusted_target = plan.current_nr_task_managers - context.max_scaling_step;


scale_up(plan) if plan.current_nr_task_managers < plan.target_nr_task_managers;
scale_down(plan) if plan.target_nr_task_managers < plan.current_nr_task_managers;


veto(plan, _context) if not scale_up(plan) and not scale_down(plan);

