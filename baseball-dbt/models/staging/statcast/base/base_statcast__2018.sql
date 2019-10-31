with source as (

    select * from {{ source('statcast', 'statcast_2018') }}

),

{{ statcast_template() }}
