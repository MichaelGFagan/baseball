with source as (

    select * from {{ source('lahman', 'fielding') }}

),

renamed as (

    select
        yearid as year_id,
        lgid as league_id,
        teamid as team_id,
        cast(stint as string) as stint,
        playerid as person_id,
        false as is_postseason,
        pos as position,
        g as games,
        gs as games_started,
        innouts as outs_at_position,
        po as putouts,
        a as assists,
        e as errors,
        dp as double_plays,
        null as triples_plays,
        pb as passed_balls,
        cast(wp as int64) as wild_pitches,
        sb as stolen_bases_allowed,
        cs as caught_stealing,
        cast(zr as int64) as zone_rating

    from source

)

select * from renamed
