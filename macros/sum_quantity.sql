{% macro sum_quantity(measure) %}

     (round(sum(total_amount), 2))

{% endmacro %}