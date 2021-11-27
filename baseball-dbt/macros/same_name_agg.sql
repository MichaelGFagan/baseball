{% macro same_name_agg(field, aggregation='sum') %}

{{ aggregation }}({{ field }}) as {{ field }}

{% endmacro %}
