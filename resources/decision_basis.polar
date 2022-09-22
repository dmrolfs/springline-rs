{{#if max_healthy_relative_lag_velocity}}
scale_up(item, _context, reason) if
    not idle_source_telemetry(item)
    and evaluation_window(window)
    and {{max_healthy_relative_lag_velocity}} < item.flow_source_relative_lag_velocity(window)
    and reason = "relative_lag_velocity";
{{/if}}

# may result in chronic decision trigger if free_task_slots out of phase with scaling step -
# possible replication of planning min_scaling_step in decision template data.
scale_down(item, _context, reason) if
    0 < item.cluster.free_task_slots
    and reason = "free_task_slots";

{{#if min_task_utilization}}
scale_down(item, _context, reason) if
    not idle_source_telemetry(item)
    and evaluation_window(window)
    and extended_window = window * 3
    and task_util_threshold = {{min_task_utilization}} / 4.0
    and item.flow_task_utilization_below_threshold(window, task_util_threshold)
    and item.flow_source_total_lag_rolling_average(extended_window) == 0.0
    and reason = "low_utilization_and_zero_lag";
{{/if}}

{{#if min_task_utilization}}
{{#if min_idle_source_back_pressured_time_millis_per_sec}}
scale_down(item, _context, reason) if
    idle_source_telemetry(item)
    and evaluation_window(window)
    and item.flow_task_utilization_below_threshold(window, {{min_task_utilization}})
    and item.flow_source_back_pressured_time_millis_per_sec_below_threshold(window, {{min_idle_source_back_pressured_time_millis_per_sec}})
    and total_lag_avg = item.flow_source_total_lag_rolling_average(window)
    and total_lag_avg == 0.0
    and reason = "low_utilization_and_idle_telemetry";
{{/if}}
{{/if}}

{{#if max_healthy_lag}}
scale_up(item, _context, reason) if
    not idle_source_telemetry(item)
    and evaluation_window(window)
    and item.flow_source_total_lag_above_threshold(window, {{max_healthy_lag}})
    and 0.0 <= item.flow_source_relative_lag_velocity(window)
    and reason = "total_lag";
{{/if}}

{{#if max_healthy_cpu_load}}
scale_up(item, _context, reason) if
    evaluation_window(window)
    and lag_increasing(item, _)
    and item.cluster_task_cpu_load_above_threshold(window, {{max_healthy_cpu_load}})
    and reason = "cpu_load";
{{/if}}

{{#if max_healthy_heap_memory_load}}
scale_up(item, _context, reason) if
    evaluation_window(window)
    and lag_increasing(item, _)
    and item.cluster_task_heap_memory_load_above_threshold(window, {{max_healthy_heap_memory_load}})
    and reason = "heap_memory_load";
{{/if}}


{{#if max_healthy_network_io_utilization}}
scale_up(item, _context, reason) if
    evaluation_window(window)
    and lag_increasing(item, _)
    and item.cluster_task_network_input_utilization_above_threshold(window, {{max_healthy_network_io_utilization}})
    and reason = "input_network_io";

scale_up(item, _context, reason) if
    evaluation_window(window)
    and lag_increasing(item, _)
    and item.cluster_task_network_output_utilization_above_threshold(window, {{max_healthy_network_io_utilization}})
    and reason = "output_network_io";
{{/if}}

# scale up to avoid source backpressure
scale_up(item, _, reason) if
    evaluation_window(window)
    and item.flow_source_back_pressured_time_millis_per_sec_rolling_average(window) == 1000.0
    and reason = "source_backpressure";
