{{#*inline "preamble"}}
scale(item, context, direction, reason) if scale_up(item, context, direction, reason) and direction = "up";

scale(item, context, direction, reason) if scale_down(item, context, direction, reason) and direction = "down";
{{/inline}}

{{>  (lookup this "basis")}}