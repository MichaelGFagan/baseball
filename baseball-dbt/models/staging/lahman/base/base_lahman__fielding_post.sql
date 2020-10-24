with source as (

    select * from {{ source('lahman', 'fielding_post') }}

),

renamed as (

    select
        s.playerid as baseball_reference_id,
        cast(s.round as string) as stint,
        s.teamid as team_id,
        s.lgid as league_id,
        cast(s.yearid as int64) as year_id,
        TRUE as is_postseason,
        s.pos as position,
        cast(s.g as int64) as games,
        cast(s.gs as int64) as games_started,
        cast(s.innouts as int64) as outs_at_position,
        cast(s.po as int64) as putouts,
        cast(s.a as int64) as assists,
        cast(s.e as int64) as errors,
        cast(s.dp as int64) as double_plays,
        cast(s.tp as int64) as triple_plays,
        cast(s.pb as int64) as passed_balls,
        null as wild_pitches,
        cast(s.sb as int64) as stolen_bases_allowed,
        cast(s.cs as int64) as caught_stealing,
        null as zone_rating

    from source as s

)

select * from renamed
