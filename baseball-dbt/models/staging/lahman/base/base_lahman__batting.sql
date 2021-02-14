with source as (

    select * from {{ source('lahman', 'batting') }}

),

transformed as (

    select
        source.playerid as lahman_id
      , cast(source.stint as string) as stint
      , source.teamid as team_id
      , source.lgid as league_id
      , cast(source.yearid as int64) as year_id
      , FALSE as is_postseason
      , cast(source.g as int64) as games
      , cast(source.ab as int64) as at_bats
      , cast(source.r as int64) as runs
      , cast(source.h as int64) as hits
      , cast(source._2b as int64) as doubles
      , cast(source._3b as int64) as triples
      , cast(source.hr as int64) as home_runs
      , cast(source.rbi as int64) as runs_batted_in
      , cast(source.sb as int64) as stolen_bases
      , cast(source.cs as int64) as caught_stealing
      , cast(source.bb as int64) as walks
      , cast(source.so as int64) as strikeouts
      , cast(source.ibb as int64) as intentional_walks
      , cast(source.hbp as int64) as hit_by_pitches
      , cast(source.sh as int64) as sacrifice_hits
      , cast(source.sf as int64) as sacrifice_flies
      , cast(source.gidp as int64) as ground_into_double_plays

    from source

),

final as (

    select
        transformed.lahman_id,
        transformed.stint,
        transformed.team_id,
        transformed.league_id,
        transformed.year_id,
        transformed.is_postseason,
        transformed.games,
        ifnull(transformed.at_bats, 0) + ifnull(transformed.walks, 0) + ifnull(transformed.hit_by_pitches, 0) + ifnull(transformed.sacrifice_hits, 0) + ifnull(transformed.sacrifice_flies, 0) as plate_appearances,
        transformed.at_bats,
        transformed.runs,
        transformed.hits,
        transformed.doubles,
        transformed.triples,
        transformed.home_runs,
        transformed.hits + transformed.walks + ifnull(transformed.hit_by_pitches, 0) as times_on_base,
        transformed.at_bats + transformed.walks + ifnull(transformed.hit_by_pitches, 0) + ifnull(transformed.sacrifice_flies, 0) as on_base_denominator,
        (transformed.at_bats - transformed.hits) + ifnull(transformed.sacrifice_hits, 0) + ifnull(transformed.sacrifice_flies, 0) + ifnull(transformed.ground_into_double_plays, 0) + ifnull(transformed.caught_stealing, 0) as outs_made,
        transformed.doubles + transformed.triples + transformed.home_runs as extra_base_hits,
        transformed.hits + transformed.doubles + 2 * transformed.triples + 3 * transformed.home_runs as total_bases,
        transformed.runs_batted_in,
        transformed.stolen_bases,
        transformed.caught_stealing,
        transformed.walks,
        transformed.strikeouts,
        transformed.intentional_walks,
        transformed.hit_by_pitches,
        transformed.sacrifice_hits,
        transformed.sacrifice_flies,
        transformed.ground_into_double_plays

    from transformed

)

select * from final
