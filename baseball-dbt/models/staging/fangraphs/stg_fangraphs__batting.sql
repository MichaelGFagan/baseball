{{
    config(
        materialized="table"
    )
}}

{% set start_year = 1871 %}
{% set end_year = 2020 %}

with source as (
            
    {% for year in range(start_year, end_year) %}
    
        {% set table_name = 'batting_' ~ year %}
    
        {% if loop.last %}
        
            select * from {{ source('fangraphs', table_name) }} as source
            
        {% else %}
        
            select * from {{ source('fangraphs', table_name) }} as source
            
            union all

        {% endif %}

    {% endfor %}
    
)

select * from source