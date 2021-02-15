with source as (

    select * from {{ source('lahman', 'pitching') }}

),

transformed as (

    select
        source.playerid as lahman_id
      , cast(source.stint as string) as stint
      , source.teamid as team_id
      , source.lgid as league_id
      , cast(source.yearid as int64) as year_id
      , FALSE as is_postseason
      , cast(source.w as int64) as wins
      , cast(source.l as int64) as losses
      , cast(source.g as int64) as games
      , cast(source.gs as int64) as games_started
      , cast(source.gf as int64) as games_finished
      , cast(source.cg as int64) as complete_games
      , cast(source.sho as int64) as shutouts
      , cast(source.sv as int64) as saves
      , cast(source.ipouts as int64) as outs_pitched
      , cast(source.h as int64) as hits
      , cast(source.r as int64) as runs
      , cast(source.er as int64) as earned_runs
      , cast(source.hr as int64) as home_runs
      , cast(source.bb as int64) as walks
      , cast(source.so as int64) as strikeouts
      , cast(source.ibb as int64) as intentional_walks
      , cast(source.wp as int64) as wild_pitches
      , cast(source.hbp as int64) as hit_by_pitches
      , cast(source.bk as int64) as balks
      , cast(source.bfp as int64) as batters_faced
      , cast(source.sh as int64) as sacrifice_hits
      , cast(source.sf as int64) as sacrifice_flies
      , cast(source.gidp as int64) as ground_into_double_plays

    from source

)

select * from transformed
