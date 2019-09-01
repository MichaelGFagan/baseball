with source as (

    select * from {{ source('lahman', 'fielding_of_split') }}

),

renamed as (

    select
        yearid as year_id,
        lgid as league_id,
        teamid as team_id,
        stint,
        playerid as person_id,
        pos as position,
        g as games,
        gs as games_started,
        innouts as outs_at_position,
        po as putouts,
        a as assists,
        e as errors,
        dp as double_plays,
        cast(pb as int64) as passed_balls,
        cast(wp as int64) as wild_pitches,
        cast(sb as int64) as stolen_bases_allowed,
        cast(cs as int64) as caught_stealing,
        cast(zr as int64) as zone_rating

    from source

)

select * from renamed
