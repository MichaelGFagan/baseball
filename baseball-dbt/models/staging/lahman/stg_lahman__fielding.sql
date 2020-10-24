with fielding as (

    select *
    from {{ ref('base_lahman__fielding') }}
    where not (cast(year_id as int64) >= 1954 and position = 'OF')

),

fielding_of_split as (

    select * from {{ ref('base_lahman__fielding_of_split') }}

),

fielding_post as (

    select * from {{ ref('base_lahman__fielding_post') }}

)

select * from fielding

union all

select * from fielding_of_split

union all

select * from fielding_post
