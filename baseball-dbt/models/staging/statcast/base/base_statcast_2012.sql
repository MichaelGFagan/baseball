with source as (

    select * from {{ source('statcast', 'statcast_2012') }}

),

{{ statcast_template() }}
