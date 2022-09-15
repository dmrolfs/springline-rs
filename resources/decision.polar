scale(item, context, direction, reason) if scale_up(item, context, reason) and direction = "up";

scale(item, context, direction, reason) if scale_down(item, context, reason) and direction = "down";

evaluation_window(window) if window = {{#if evaluate_duration_secs}}{{evaluate_duration_secs}}{{else}}60{{/if}};

idle_source_telemetry(item) if
  item.flow.source_total_lag == nil
  or item.flow.source_records_lag_max == nil
  or item.flow.source_assigned_partitions == nil;

lag_increasing(item, velocity) if
  evaluation_window(window)
  and not idle_source_telemetry(item)
  and velocity = item.flow_source_relative_lag_velocity(window)
  and 0.0 < velocity;

lag_decreasing(item, velocity) if
  evaluation_window(window)
  and not idle_source_telemetry(item)
  and velocity = item.flow_source_relative_lag_velocity(window)
  and velocity < 0.0;

{{>  (lookup this "basis")}}


# no action rules to avoid policy issues if corresponding up/down rules not specified in basis.polar
scale_up(_, _, _) if false;
scale_down(_, _, _) if false;
