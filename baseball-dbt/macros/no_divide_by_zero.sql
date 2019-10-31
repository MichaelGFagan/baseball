{% macro no_divide_by_zero(numerator, denominator, decimal) %}

case
    when {{ denominator }} = 0 then round(0, {{ decimal }})
    else round(safe_divide({{ numerator }}, {{ denominator }}), {{ decimal }})
end

{% endmacro %}
