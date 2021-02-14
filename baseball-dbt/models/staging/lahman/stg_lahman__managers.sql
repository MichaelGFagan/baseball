with source as (

    select * from {{ source('lahman', 'managers') }}

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
      , s.inseason as order_in_season
      , s.g as games
      , s.w as wins
      , s.l as losses
      , s.rank as position_in_standings
      , s.plyrmgr as is_player_manager

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
