{{> preamble}}

{{#if max_healthy_lag}}
scale_up(item, _context, _, reason) if
    lag in item.flow.input_records_lag_max
    and {{max_healthy_lag}} < lag
    and reason = "kafka_lag";
{{/if}}

{{#if max_healthy_cpu_load}}
scale_up(item, _context, _, reason) if
    item.cluster_task_cpu_load_above_mark(180, {{max_healthy_cpu_load}})
    and reason = "cpu_load";
#    {{max_healthy_cpu_load}} < (item.cluster.task_cpu_load / item.cluster.nr_task_managers)
{{/if}}

{{#if max_healthy_heap_memory_load}}
scale_up(item, _context, _, reason) if
    {{max_healthy_heap_memory_load}} < item.cluster.task_heap_memory_load()
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

{{#if min_healthy_lag}}
scale_down(item, _context, _, reason) if
    lag in item.flow.input_records_lag_max
    and lag < {{min_healthy_lag}}
    and reason = "kafka_lag";
{{/if}}

{{#if min_healthy_cpu_load}}
scale_down(item, _, _, reason) if
    item.cluster_task_cpu_load_below_mark(180, {{min_healthy_cpu_load}})
    and reason = "cpu_load";
# (item.cluster.task_cpu_load / item.cluster.nr_task_managers) < {{min_healthy_cpu_load}}
{{/if}}