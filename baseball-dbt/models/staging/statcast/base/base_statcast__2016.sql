with source as (

    select * from {{ source('statcast', 'statcast_2016') }}

),

{{ statcast_template() }}
