with fielding as (

    select *
    from {{ ref('base_lahman__fielding') }}
    where not (year_id >= 1954 and position = 'OF')

),

fielding_of_split as (

    select * from {{ ref('base_lahman__fielding_of_split') }}

)

select * from fielding

union all

select * from fielding_of_split
