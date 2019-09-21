with batting as (

    select * from {{ ref('base_lahman__batting') }}

),

batting_post as (

    select * from {{ ref('base_lahman__batting_post') }}

)

select * from batting

union

select * from batting_post
