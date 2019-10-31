with source as (

    select * from {{ source('statcast', 'statcast_2019') }}

),

{{ statcast_template() }}
