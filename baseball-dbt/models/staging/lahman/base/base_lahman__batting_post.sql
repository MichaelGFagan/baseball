with source as (

    select * from {{ source('lahman', 'batting_post') }}

),

renamed as (

    select
        s.playerid as baseball_reference_id,
        cast(s.round as string) as stint,
        s.teamid as team_id,
        s.lgid as league_id,
        cast(s.yearid as int64) as year_id,
        TRUE as is_postseason,
        cast(s.g as int64) as games,
        cast(s.ab as int64) as at_bats,
        cast(s.r as int64) as runs,
        cast(s.h as int64) as hits,
        cast(s._2b as int64) as doubles,
        cast(s._3b as int64) as triples,
        cast(s.hr as int64) as home_runs,
        cast(s.rbi as int64) as runs_batted_in,
        cast(s.sb as int64) as stolen_bases,
        cast(s.cs as int64) as caught_stealing,
        cast(s.bb as int64) as walks,
        cast(s.so as int64) as strikeouts,
        cast(s.ibb as int64) as intentional_walks,
        cast(s.hbp as int64) as hit_by_pitch,
        cast(s.sh as int64) as sacrifice_hits,
        cast(s.sf as int64) as sacrifice_flies,
        cast(s.gidp as int64) as ground_into_double_plays

    from source as s

),

final as (

    select
        r.baseball_reference_id,
        r.stint,
        r.team_id,
        r.league_id,
        r.year_id,
        r.is_postseason,
        r.games,
        ifnull(r.at_bats, 0) + ifnull(r.walks, 0) + ifnull(r.hit_by_pitch, 0) + ifnull(r.sacrifice_hits, 0) + ifnull(r.sacrifice_flies, 0) as plate_appearances,
        r.at_bats,
        r.runs,
        r.hits,
        r.doubles,
        r.triples,
        r.home_runs,
        r.hits + r.walks + ifnull(r.hit_by_pitch, 0) as times_on_base,
        r.at_bats + r.walks + ifnull(r.hit_by_pitch, 0) + ifnull(r.sacrifice_flies, 0) as on_base_denominator,
        (r.at_bats - r.hits) + ifnull(r.sacrifice_hits, 0) + ifnull(r.sacrifice_flies, 0) + ifnull(r.ground_into_double_plays, 0) + ifnull(r.caught_stealing, 0) as outs_made,
        r.doubles + r.triples + r.home_runs as extra_base_hits,
        r.hits + r.doubles + 2 * r.triples + 3 * r.home_runs as total_bases,
        r.runs_batted_in,
        r.stolen_bases,
        r.caught_stealing,
        r.walks,
        r.strikeouts,
        r.intentional_walks,
        r.hit_by_pitch,
        r.sacrifice_hits,
        r.sacrifice_flies,
        r.ground_into_double_plays

    from renamed as r

)

select * from final
