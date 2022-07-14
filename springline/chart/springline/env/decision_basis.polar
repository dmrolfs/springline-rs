{{> preamble}}

{{#if max_healthy_relative_lag_velocity}}
scale_up(item, _context, _, reason) if
    not item.flow.input_records_lag_max == nil
    and not item.flow.input_assigned_partitions == nil
    and evaluation_window(window)
    and relative_lag_velocity = item.flow_input_relative_lag_change_rate(window)
    and {{max_healthy_relative_lag_velocity}} < relative_lag_velocity
    and reason = "relative_lag_velocity";
{{/if}}


{{#if min_task_utilization}}
scale_down(item, _context, _, reason) if
    not item.flow.input_records_lag_max == nil
    and not item.flow.input_assigned_partitions == nil
    and evaluation_window(window)
    and utilization = item.flow_task_utilization_rolling_average(window)
    and utilization < {{min_task_utilization}}
    and total_lag = item.flow_input_total_lag_rolling_average(window)
    and total_lag == 0.0
    and reason = "low_utilization_and_zero_lag";
{{/if}}

# add input_backpressured_time_ms_per_sec order on source operator
# {{#if min_task_utilization}}
# scale_down(item, _context, _, reason) if
#     not item.flow.input_backpressured_time_ms_per_sec == nil
#     and evaluation_window(window)
#     and utilization = item.flow_task_utilization_rolling_average(window)
#     and utilization < {{min_task_utilization}}
#     and total_lag = item.flow_input_total_lag_rolling_average(window)
#     and total_lag == 0.0
#     and reason = "low_utilization_and_zero_lag";
# {{/if}}

evaluation_window(window) if
    window = {{#if evaluate_duration_secs}}
	{{evaluate_duration_secs}}
    {{else}}
	60
    {{/if}};


{{#if max_healthy_lag}}
scale_up(item, _context, _, reason) if
    not item.flow.input_records_lag_max == nil
    and evaluation_window(window)
    and lag = item.flow_input_total_lag_rolling_average(window)
    and {{max_healthy_lag}} < lag
    and reason = "total_lag";
{{/if}}

{{#if max_healthy_cpu_load}}
scale_up(item, _context, _, reason) if
    evaluation_window(window)
    and item.cluster_task_cpu_load_above_mark(window, {{max_healthy_cpu_load}})
    and reason = "cpu_load";
{{/if}}

{{#if max_healthy_heap_memory_load}}
scale_up(item, _context, _, reason) if
    evaluation_window(window)
    and load = item.cluster_task_heap_memory_load_rolling_average(window)
    and healthy_heap_memory_load}} < load
    and reason = "heap_memory_load";
{{/if}}

{{#if max_healthy_network_io_utilization}}
scale_up(item, _context, _, reason) if
    {{max_healthy_network_io_utilization}} < item.cluster.task_network_input_utilization()
    and reason = "input_network_io";

scale_up(item, _context, _, reason) if
    {{max_healthy_network_io_utilization}} < item.cluster.task_network_output_utilization()
    and reason = "output_network_io";
{{/if}}

