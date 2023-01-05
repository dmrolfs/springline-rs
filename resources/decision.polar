scale(item, context, direction, reason) if scale_up(item, context, reason) and direction = "up";

scale(item, context, direction, reason) if scale_down(item, context, reason) and direction = "down";

# returns the evaluation window in seconds to be used - e.g., a rolling average window - in other decision rules.
# The default evaluation window is 60 seconds.
evaluation_window(window) if window = {{#if evaluate_duration_secs}}{{evaluate_duration_secs}}{{else}}60{{/if}};

# returns the configured minimal task utilization.
# The default minimal task utilization is 0.25.
min_utilization(utilization) if utilization = {{#if min_task_utilization}}{{min_task_utilization}}{{else}}0.25{{/if}};

# evaluates to true if source telemetry is not available or considered idle.
idle_source_telemetry(item) if
  item.flow.source_total_lag == nil
  or item.flow.source_records_lag_max == nil
  or item.flow.source_assigned_partitions == nil;

# evaluates to true if the lag is determined to be increasing relative to the job's ability to
# process. The relative lag velocity, evaluated over the evaluation window, is returned.
lag_increasing(item, velocity) if
  evaluation_window(window)
  and not idle_source_telemetry(item)
  and velocity = item.flow_source_relative_lag_velocity(window)
  and 0.0 < velocity;

# evaluates to true if the lag is determined to be decreasing relative to the job's ability to
# process. The relative lag velocity, evaluated over the evaluation window, is returned.
lag_decreasing(item, velocity) if
  evaluation_window(window)
  and not idle_source_telemetry(item)
  and velocity = item.flow_source_relative_lag_velocity(window)
  and velocity < 0.0;

{{>  (lookup this "basis")}}


# no action rules to avoid policy issues if corresponding up/down rules not specified in basis.polar
scale_up(_, _, _) if false;
scale_down(_, _, _) if false;
