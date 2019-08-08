with source as (

    select * from {{ source(baseball_source, batting) }}

),

renamed as (

    select
        concat(cast(yearid as string), lgid, teamid, '0', cast(stint as string), playerid) as id,
        yearid as year_id,
        lgid as league_id,
        teamid as team_id,
        stint,
        playerid as player_id,
        g as games,
        (ab + bb + hbp + sf + sh) as plate_appearances
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
        h / ab as batting_average,
        (h + bb + hbp) / (ab + bb + hbp + sf) as on_base_percentage,
        (h + _2b + 3 * _3b + 4 * hr) / ab as slugging_percentage,
        (h + bb + hbp) / (ab + bb + hbp + sf) + (h + _2b + 3 * _3b + 4 * hr) / ab as on_base_plus_slugging,
        h + _2b + 3 * _3b + 4 * hr as total_bases,
        h + _2b + 3 * _3b + 4 * hr + sb + sh + sf + as total_bases,
        h + bb + hbp as times_on_base,
        (ab - h) + sh + sf + gidp as outs_hit_into,
        ibb as intentional_walks,
        hbp as hit_by_pitches,
        sh as sacrifice_hits,
        sf as sacrifice_flies,
        gidp as ground_into_double_plays

    from source

)

select * from renamed
