{% macro nvl2(field, if_not_null, if_null) %}

case
    when {{ field }} is not null then {{ if_not_null }}
    else {{ if_null }}
end

{% endmacro %}
