with source as (

    select * from {{ source('statcast', 'statcast_2009') }}

),

{{ statcast_template() }}
