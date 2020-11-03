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

transformed as (

    select
        c.person_id,
        u.*

    from unioned as u
    left join chadwick as c using (baseball_reference_id)

)

select * from transformed
