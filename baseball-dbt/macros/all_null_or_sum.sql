{% macro all_null_or_sum(field) %}

case
    when sum({{ nvl2(field, 0, 1) }}) = count(*) then null
    else sum(ifnull({{ field }}, 0))
end

{% endmacro %}
