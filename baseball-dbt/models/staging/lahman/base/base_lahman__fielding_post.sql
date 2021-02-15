with source as (

    select * from {{ source('lahman', 'fielding_post') }}

),

transformed  as (

    select
        source.playerid as lahman_id
      , cast(source.round as string) as stint
      , source.teamid as team_id
      , source.lgid as league_id
      , cast(source.yearid as int64) as year_id
      , TRUE as is_postseason
      , source.pos as position
      , case
            when source.pos = 'P' then 1
            when source.pos = 'C' then 2
            when source.pos = '1B' then 3
            when source.pos = '2B' then 4
            when source.pos = '3B' then 5
            when source.pos = 'SS' then 6
            when source.pos = 'LF' then 7
            when source.pos = 'CF' then 8
            when source.pos = 'RF' then 9
        end as position_number
      , cast(source.g as int64) as games
      , cast(source.gs as int64) as games_started
      , cast(source.innouts as int64) as outs_at_position
      , cast(source.po as int64) as putouts
      , cast(source.a as int64) as assists
      , cast(source.e as int64) as errors
      , cast(source.dp as int64) as double_plays
      , cast(source.tp as int64) as triple_plays
      , cast(source.pb as int64) as passed_balls
      , null as wild_pitches
      , cast(source.sb as int64) as stolen_bases_allowed
      , cast(source.cs as int64) as caught_stealing
      , null as zone_rating

    from source

)

select * from transformed
