with source as (

    select * from {{ source('statcast', 'statcast_2020') }}

),

{{ statcast_template() }}
