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
        cs as caught_stealing,
        bb as walks,
        so as strikeouts,
        cast(ibb as int64) as intentional_walks,
        cast(hbp as int64) as hit_by_pitch,
        cast(sh as int64) as sacrifice_hits,
        cast(sf as int64) as sacrifice_flies,
        gidp as ground_into_double_plays

    from source

),

final as (

    select
        year_id,
        league_id,
        team_id,
        stint,
        player_id,
        games,
        ifnull(at_bats, 0) + ifnull(walks, 0) + ifnull(hit_by_pitch, 0) + ifnull(sacrifice_hits, 0) + ifnull(sacrifice_flies, 0) as plate_appearances,
        at_bats,
        runs,
        hits,
        doubles,
        triples,
        home_runs,
        hits + walks + ifnull(hit_by_pitch, 0) as times_on_base,
        at_bats + walks + ifnull(hit_by_pitch, 0) + ifnull(sacrifice_flies, 0) as on_base_denominator,
        (at_bats - hits) + ifnull(sacrifice_hits, 0) + ifnull(sacrifice_flies, 0) + ifnull(ground_into_double_plays, 0) as batting_outs,
        hits + doubles + 2 * triples + 3 * home_runs as total_bases,
        runs_batted_in,
        stolen_bases,
        caught_stealing,
        walks,
        strikeouts,
        intentional_walks,
        hit_by_pitch,
        sacrifice_hits,
        sacrifice_flies,
        ground_into_double_plays

    from renamed

)

select * from final
