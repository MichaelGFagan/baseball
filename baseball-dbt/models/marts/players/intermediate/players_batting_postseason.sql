with batting as (

    select * from {{ ref('stg_lahman__batting') }} where is_postseason = TRUE

),

sums as (

    select
        batting.person_id
      , {{ all_null_or_sum('batting.games') }} as games
      , {{ all_null_or_sum('batting.plate_appearances') }} as plate_appearances
      , {{ all_null_or_sum('batting.at_bats') }} as at_bats
      , {{ all_null_or_sum('batting.runs') }} as runs
      , {{ all_null_or_sum('batting.hits') }} as hits
      , {{ all_null_or_sum('batting.doubles') }} as doubles
      , {{ all_null_or_sum('batting.triples') }} as triples
      , {{ all_null_or_sum('batting.home_runs') }} as home_runs
      , {{ all_null_or_sum('batting.times_on_base') }} as times_on_base
      , {{ all_null_or_sum('batting.outs_made') }} as outs_made
      , {{ all_null_or_sum('batting.extra_base_hits') }} as extra_base_hits
      , {{ all_null_or_sum('batting.total_bases') }} as total_bases
      , {{ all_null_or_sum('batting.runs_batted_in') }} as runs_batted_in
      , {{ all_null_or_sum('batting.stolen_bases') }} as stolen_bases
      , {{ all_null_or_sum('batting.caught_stealing') }} as caught_stealing
      , {{ all_null_or_sum('batting.walks') }} as walks
      , {{ all_null_or_sum('batting.strikeouts') }} as strikeouts
      , {{ all_null_or_sum('batting.intentional_walks') }} as intentional_walks
      , {{ all_null_or_sum('batting.hit_by_pitches') }} as hit_by_pitches
      , {{ all_null_or_sum('batting.sacrifice_hits') }} as sacrifice_hits
      , {{ all_null_or_sum('batting.sacrifice_flies') }} as sacrifice_flies
      , {{ all_null_or_sum('batting.ground_into_double_plays') }} as ground_into_double_plays
      , {{ all_null_or_sum('batting.on_base_denominator') }} as on_base_denominator

    from batting

    group by 1

),

transformed as (

    select
        sums.person_id
        {# sums.seasons as postseason_appearances,
        sums.franchises as postseason_franchises, #}
      , sums.games as postseason_games_batted
      , sums.plate_appearances as postseason_plate_appearances
      , sums.at_bats as postseason_at_bats
      , sums.runs as postseason_runs
      , sums.hits as postseason_hits
      , sums.doubles as postseason_doubles
      , sums.triples as postseason_triples
      , sums.home_runs as postseason_home_runs
      , sums.times_on_base as postseason_times_on_base
      , sums.outs_made as postseason_outs_made
      , sums.extra_base_hits as postseason_extra_base_hits
      , sums.total_bases as postseason_total_bases
      , sums.runs_batted_in as postseason_runs_batted_in
      , sums.stolen_bases as postseason_stolen_bases
      , sums.caught_stealing as postseason_caught_stealing
      , sums.walks as postseason_walks
      , sums.strikeouts as postseason_strikeouts
      , {{ no_divide_by_zero('sums.hits', 'sums.at_bats', 3) }} as postseason_batting_average
      , {{ no_divide_by_zero('sums.times_on_base', 'sums.on_base_denominator', 3) }} as postseason_on_base_percentage
      , {{ no_divide_by_zero('sums.total_bases', 'sums.at_bats', 3) }} as postseason_slugging_percentage
      , round({{ no_divide_by_zero('sums.times_on_base', 'sums.on_base_denominator', 9) }} + {{ no_divide_by_zero('sums.total_bases', 'sums.at_bats', 9) }}, 3) as on_base_plus_sluggin
      , sums.intentional_walks as postseason_intentional_walks
      , sums.hit_by_pitches as postseason_hit_by_pitches
      , sums.sacrifice_hits as postseason_sacrifice_hits
      , sums.sacrifice_flies as postseason_sacrifice_flies
      , sums.ground_into_double_plays as postseason_ground_into_double_plays
      , {{ no_divide_by_zero('sums.home_runs', 'sums.plate_appearances', 3) }} as postseason_home_run_rate
      , {{ no_divide_by_zero('sums.extra_base_hits', 'sums.plate_appearances', 3) }} as postseason_extra_base_hit_rate
      , {{ no_divide_by_zero('sums.strikeouts', 'sums.plate_appearances', 3) }} as postseason_strikeout_rate
      , {{ no_divide_by_zero('sums.walks', 'sums.plate_appearances', 3) }} as postseason_walk_rate
      , {{ no_divide_by_zero('sums.walks', 'sums.strikeouts', 3) }} as postseason_walk_per_strikeout
      , {{ no_divide_by_zero('sums.strikeouts', 'sums.walks', 3) }} as postseason_strikeout_per_walk

    from sums

)

select * from transformed
