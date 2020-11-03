with pitching as (

    select * from {{ ref('base_lahman__pitching') }}

),

pitching_post as (

    select * from {{ ref('base_lahman__pitching_post') }}

),

chadwick as (

    select * from {{ ref('util_chadwick__register') }}

),

unioned as (

    select * from pitching

    union all

    select * from pitching_post

),

transformed as (

    select
        c.person_id,
        u.*

    from unioned as u
    left join chadwick as c using (baseball_reference_id)

)

select * from transformed
