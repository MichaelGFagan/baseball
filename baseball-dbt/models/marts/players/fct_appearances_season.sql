with appearances as (

    select * from {{ ref('stg_lahman__appearances') }}

)

select * from appearances
