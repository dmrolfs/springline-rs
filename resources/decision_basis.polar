{{#if max_healthy_relative_lag_velocity}}
scale_up(item, _context, reason) if
    not item.flow.source_records_lag_max == nil
    and not item.flow.source_assigned_partitions == nil
    and evaluation_window(window)
    and {{max_healthy_relative_lag_velocity}} < item.flow_source_relative_lag_velocity(window)
    and reason = "relative_lag_velocity";
{{/if}}


{{#if min_task_utilization}}
scale_down(item, _context, reason) if
    not item.flow.source_records_lag_max == nil
    and not item.flow.source_assigned_partitions == nil
    and evaluation_window(window)
    and item.flow_task_utilization_below_threshold(window, {{min_task_utilization}})
    and item.flow_source_total_lag_rolling_average(window) == 0.0
    and reason = "low_utilization_and_zero_lag";
{{/if}}

{{#if min_task_utilization}}
scale_down(item, _context, reason) if
    item.flow.source_total_lag == nil
    and item.flow.source_records_consumed_rate == nil
    and item.flow.source_records_lag_max == nil
    and item.flow.source_assigned_partitions == nil
    and evaluation_window(window)
    and item.flow_task_utilization_below_threshold(window, {{min_task_utilization}})
    and item.flow_source_back_pressured_time_millis_per_sec_below_threshold(window, 50.0)
    and total_lag_avg = item.flow_source_total_lag_rolling_average(window)
    and total_lag_avg == 0.0
    and reason = "low_utilization_and_idle_telemetry";
{{/if}}

evaluation_window(window) if window = {{#if evaluate_duration_secs}}{{evaluate_duration_secs}}{{else}}60{{/if}};

{{#if max_healthy_lag}}
scale_up(item, _context, reason) if
    not item.flow.source_records_lag_max == nil
    and not item.flow.source_assigned_partitions == nil
    and evaluation_window(window)
    and item.flow_source_total_lag_above_threshold(window, {{max_healthy_lag}})
    and 0.0 <= item.flow_source_relative_lag_velocity(window)
    and reason = "total_lag";
{{/if}}

{{#if max_healthy_cpu_load}}
scale_up(item, _context, reason) if
    evaluation_window(window)
    and item.cluster_task_cpu_load_above_threshold(window, {{max_healthy_cpu_load}})
    and reason = "cpu_load";
{{/if}}

{{#if max_healthy_heap_memory_load}}
scale_up(item, _context, reason) if
    evaluation_window(window)
    and item.cluster_task_heap_memory_load_above_threshold(window, {{max_healthy_heap_memory_load}})
    and reason = "heap_memory_load";
{{/if}}


{{#if max_healthy_network_io_utilization}}
scale_up(item, _context, reason) if
    evaluation_window(window)
    and item.cluster_task_network_input_utilization_above_threshold(window, {{max_healthy_network_io_utilization}})
    and reason = "input_network_io";

scale_up(item, _context, reason) if
    evaluation_window(window)
    and item.cluster_task_network_output_utilization_above_threshold(window, {{max_healthy_network_io_utilization}})
    and reason = "output_network_io";
{{/if}}

# scale up to avoid source backpressure
scale_up(item, _, reason) if
    evaluation_window(window)
    and item.flow_source_back_pressured_time_millis_per_sec_rolling_average(window) == 1000.0
    and reason = "source_backpressure";
