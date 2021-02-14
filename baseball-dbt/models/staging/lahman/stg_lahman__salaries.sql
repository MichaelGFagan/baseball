with source as (

    select * from {{ source('lahman', 'salaries') }}

),

chadwick as (

    select * from {{ ref('util_chadwick__register') }}

),

transformed as (

    select
        s.playerid as lahman_id
      , s.teamid as team_id
      , s.lgid as league_id
      , cast(s.yearid as int64) as year_id
      , s.salary

    from source as s

),

final as (

    select
        chadwick.person_id
      , transformed.*

    from transformed
    left join chadwick
        on transformed.lahman_id = chadwick.lahman_id

)

select * from final
