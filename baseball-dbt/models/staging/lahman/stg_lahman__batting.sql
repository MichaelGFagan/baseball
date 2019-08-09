with source as (

    select * from {{ source('lahman', 'batting') }}

),

renamed as (

    select
        yearid as year_id,
        lgid as league_id,
        teamid as team_id,
        stint,
        playerid as player_id,
        g as games,
        ab as at_bats,
        r as runs,
        h as hits,
        _2b as doubles,
        _3b as triples,
        hr as home_runs,
        rbi as runs_batted_in,
        sb as stolen_bases,
        cs as caught_stealings,
        bb as walks,
        so as strikeouts,
        cast(ibb as int64) as intentional_walks,
        cast(hbp as int64) as hit_by_pitches,
        cast(sh as int64) as sacrifice_hits,
        cast(sf as int64) as sacrifice_flies,
        gidp as ground_into_double_plays

    from source

)

select * from renamed
