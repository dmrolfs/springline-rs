scale(item, context, direction, reason) if scale_up(item, context, reason) and direction = "up";

scale(item, context, direction, reason) if scale_down(item, context, reason) and direction = "down";

{{>  (lookup this "basis")}}


# no action rules to avoid policy issues if corresponding up/down rules not specified in basis.polar
scale_up(_, _, _) if false;
scale_down(_, _, _) if false;
