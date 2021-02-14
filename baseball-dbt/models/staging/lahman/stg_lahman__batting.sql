with batting as (

    select * from {{ ref('base_lahman__batting') }}

),

batting_post as (

    select * from {{ ref('base_lahman__batting_post') }}

),

chadwick as (

    select * from {{ ref('util_chadwick__register') }}

),

unioned as (

    select * from batting

    union all

    select * from batting_post

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
