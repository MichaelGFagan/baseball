with source as (

    select * from {{ source('statcast', 'statcast_2015') }}

),

{{ statcast_template() }}
