{{> preamble}}

scale_up(item, _context, _) if {{max_healthy_lag}} < item.flow.input_records_lag_max;

scale_up(item, _context, _) if {{max_healthy_cpu}} < item.cluster.task_cpu_load;

scale_up(item, _context, _) if {{max_healthy_network_io}} < item.cluster.network_io_utilization;

scale_up(item, _context, _) if
    {{max_healthy_network_io}} < (item.cluster.task_network_input_buffer_usage / item.cluster.task_network_input_buffer_len) or
    {{max_healthy_network_io}} < (item.cluster.task_network_output_buffer_usage / item.cluster.task_network_output_buffer_len);

scale_down(item, _context, _) if item.flow.input_records_lag_max < {{min_healthy_lag}};
