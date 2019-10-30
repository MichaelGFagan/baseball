with source as (

    select * from {{ source('statcast', 'statcast_2010') }}

),

{{ statcast_template() }}
