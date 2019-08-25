with source as (

    select * from {{ source('lahman', 'pitching') }}

),

renamed as (

    select
        yearid as year_id,
        lgid as league_id,
        teamid as team_id,
        stint,
        playerid as person_id,
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
        ibb as intentional_walks,
        wp as wild_pitches,
        hbp as hit_by_pitch,
        bk as balks,
        bfp as batters_faced,
        sh as sacrifice_hits,
        sf as sacrifice_flies,
        gidp as ground_into_double_plays

    from source

),

final as (

    select
        year_id,
        league_id,
        team_id,
        stint,
        person_id,
        wins,
        losses,
        games,
        games_started,
        games_finished,
        complete_games,
        shutouts,
        saves,
        outs_pitched,
        hits,
        runs,
        earned_runs,
        home_runs,
        walks,
        strikeouts,
        cast(intentional_walks as int64) as intentional_walks,
        wild_pitches,
        hit_by_pitch,
        balks,
        batters_faced,
        cast(sacrifice_hits as int64) as sacrifice_hits,
        cast(sacrifice_flies as int64) as sacrifice_flies,
        cast(ground_into_double_plays as int64) as ground_into_double_plays

    from renamed

)

select * from final
