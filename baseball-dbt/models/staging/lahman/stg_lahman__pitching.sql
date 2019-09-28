with pitching as (

    select * from {{ ref('base_lahman__pitching') }}

),

pitching_post as (

    select * from {{ ref('base_lahman__pitching_post') }}

)

select * from pitching

union all

select * from pitching_post
