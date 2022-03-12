{{> preamble}}

eligible(item, context, reason) if not ineligible(item, context, root_reason) and reason = root_reason;

ineligible(_item, context, reason) if context.cluster_status.is_rescaling and reason = "rescaling";
ineligible(item, context, reason) if deploying(item, context) and reason = "deploying";
ineligible(item, context, reason) if recent_failure(item, context) and reason = "recent_failure";
ineligible(item, context, reason) if in_cooling_period(item, context) and reason = "cooling_period";
