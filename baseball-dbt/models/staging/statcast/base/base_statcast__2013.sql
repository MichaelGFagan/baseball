with source as (

    select * from {{ source('statcast', 'statcast_2013') }}

),

{{ statcast_template() }}
