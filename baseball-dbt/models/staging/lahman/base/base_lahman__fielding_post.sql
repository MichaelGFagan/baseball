with source as (

    select * from {{ source('lahman', 'fielding_post') }}

),

renamed as (

    select
        yearid as year_id,
        lgid as league_id,
        teamid as team_id,
        round as stint,
        playerid as person_id,
        true as is_postseason,
        pos as position,
        g as games,
        gs as games_started,
        innouts as outs_at_position,
        po as putouts,
        a as assists,
        e as errors,
        dp as double_plays,
        tp as triple_plays,
        pb as passed_balls,
        null as wild_pitches,
        cast(sb as int64) as stolen_bases_allowed,
        cast(cs as int64) as caught_stealing,
        null as zone_rating

    from source

)

select * from renamed
