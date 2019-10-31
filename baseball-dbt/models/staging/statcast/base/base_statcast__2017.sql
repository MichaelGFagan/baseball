with source as (

    select * from {{ source('statcast', 'statcast_2017') }}

),

{{ statcast_template() }}
