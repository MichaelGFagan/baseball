with managers as (

    select * from {{ ref('stg_lahman__managers') }}

)

select
    m.lahman_id,
    m.year_id,
    m.team_id,
    m.league_id,
    m.order_in_season,
    m.games,
    m.wins,
    m.losses,
    m.position_in_standings,
    m.is_player_manager

from managers as m
