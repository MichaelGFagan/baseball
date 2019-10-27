with source as (

    select * from {{ source('lahman', 'fielding_post') }}

),

renamed as (

    select
        s.playerid as baseball_reference_id,
        s.round as stint,
        s.teamid as team_id,
        s.lgid as league_id,
        s.yearid as year_id,
        TRUE as is_postseason,
        s.pos as position,
        s.g as games,
        s.gs as games_started,
        s.innouts as outs_at_position,
        s.po as putouts,
        s.a as assists,
        s.e as errors,
        s.dp as double_plays,
        s.tp as triple_plays,
        s.pb as passed_balls,
        null as wild_pitches,
        cast(s.sb as int64) as stolen_bases_allowed,
        cast(s.cs as int64) as caught_stealing,
        null as zone_rating

    from source as s

)

select * from renamed
