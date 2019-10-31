with source as (

    select * from {{ source('statcast', 'statcast_2014') }}

),

{{ statcast_template() }}
