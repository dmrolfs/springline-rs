{{> preamble}}

eligible(item, context, reason) if not ineligible(item, context, root_reason) and reason = root_reason;

ineligible(item, _context, reason) if item.cluster.nr_active_jobs == 0 and reason = "no_active_jobs";
ineligible(_item, context, reason) if context.cluster.is_rescaling and reason = "rescaling";
ineligible(item, context, reason) if deploying(item, context) and reason = "deploying";
ineligible(item, context, reason) if recent_failure(item, context) and reason = "recent_failure";
ineligible(item, context, reason) if in_cooling_period(item, context) and reason = "cooling_period";
