with managers as (

    select * from {{ ref('stg_lahman__managers') }}

)

select
    managers.lahman_id
  , managers.year_id
  , managers.team_id
  , managers.league_id
  , managers.order_in_season
  , managers.games
  , managers.wins
  , managers.losses
  , managers.position_in_standings
  , managers.is_player_manager

from managers
