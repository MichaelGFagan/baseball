with source as (

    select * from {{ source('statcast', 'statcast_2011') }}

),

{{ statcast_template() }}
