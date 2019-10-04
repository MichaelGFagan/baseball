{% macro no_divide_by_zero(numerator, denominator, decimal) %}

case
    when {{ denominator }} = 0 then round(0, {{ decimal }})
    else round({{ numerator }} / {{ denominator }}, {{ decimal }})
end

{% endmacro %}
