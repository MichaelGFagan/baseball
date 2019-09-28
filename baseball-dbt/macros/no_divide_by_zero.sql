{% macro sum_or_null(numerator, denominator, decimal) %}

case
    when denominator is null then round(0, decimal)
    else round(numerator / denominator, decimal)
end

{% endmacro %}
