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

),

chadwick as (

    select * from {{ ref('util_chadwick__register') }}

),

unioned as (

    select * from fielding

    union all

    select * from fielding_of_split

    union all

    select * from fielding_post

),

final as (

    select
        chadwick.person_id
      , unioned.*

    from unioned
    left join chadwick
        on unioned.lahman_id = chadwick.lahman_id

)

select * from final
