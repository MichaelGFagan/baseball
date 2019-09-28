with source as (

    select * from {{ source('lahman', 'pitching') }}

),

renamed as (

    select
        yearid as year_id,
        lgid as league_id,
        teamid as team_id,
        cast(stint as string) as stint,
        playerid as person_id,
        false as is_postseason,
        w as wins,
        l as losses,
        g as games,
        gs as games_started,
        gf as games_finished,
        cg as complete_games,
        sho as shutouts,
        sv as saves,
        ipouts as outs_pitched,
        h as hits,
        r as runs,
        er as earned_runs,
        hr as home_runs,
        bb as walks,
        so as strikeouts,
        cast(ibb as int64) as intentional_walks,
        wp as wild_pitches,
        cast(hbp as int64) as hit_by_pitch,
        bk as balks,
        bfp as batters_faced,
        cast(sh as int64) as sacrifice_hits,
        cast(sf as int64) as sacrifice_flies,
        cast(gidp as int64) as ground_into_double_plays

    from source

)

select * from renamed
