with source as (

    select * from {{ source('statcast', 'statcast_2008') }}

),

{{ statcast_template() }}
