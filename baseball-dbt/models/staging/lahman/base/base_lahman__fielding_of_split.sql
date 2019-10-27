with source as (

    select * from {{ source('lahman', 'fielding_of_split') }}

),

renamed as (

    select
        s.playerid as baseball_reference_id,
        cast(s.stint as string) as stint,
        s.teamid as team_id,
        s.lgid as league_id,
        s.yearid as year_id,
        FALSE as is_postseason,
        s.pos as position,
        s.g as games,
        s.gs as games_started,
        s.innouts as outs_at_position,
        s.po as putouts,
        s.a as assists,
        s.e as errors,
        s.dp as double_plays,
        null as triple_plays,
        cast(s.pb as int64) as passed_balls,
        cast(s.wp as int64) as wild_pitches,
        cast(s.sb as int64) as stolen_bases_allowed,
        cast(s.cs as int64) as caught_stealing,
        cast(s.zr as int64) as zone_rating

    from source as s

)

select * from renamed
