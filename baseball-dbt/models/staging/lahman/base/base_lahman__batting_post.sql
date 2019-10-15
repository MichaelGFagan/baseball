with source as (

    select * from {{ source('lahman', 'batting_post') }}

),

renamed as (

    select
        s.yearid as year_id,
        s.lgid as league_id,
        s.teamid as team_id,
        s.round as stint,
        s.playerid as person_id,
        TRUE as is_postseason,
        s.g as games,
        s.ab as at_bats,
        s.r as runs,
        s.h as hits,
        s._2b as doubles,
        s._3b as triples,
        s.hr as home_runs,
        s.rbi as runs_batted_in,
        s.sb as stolen_bases,
        s.cs as caught_stealing,
        s.bb as walks,
        s.so as strikeouts,
        cast(s.ibb as int64) as intentional_walks,
        cast(s.hbp as int64) as hit_by_pitch,
        cast(s.sh as int64) as sacrifice_hits,
        cast(s.sf as int64) as sacrifice_flies,
        s.gidp as ground_into_double_plays

    from source as s

),

final as (

    select
        r.year_id,
        r.league_id,
        r.team_id,
        r.stint,
        r.person_id,
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
