with batting as (

    select * from {{ ref('stg_lahman__batting') }}

),

baseball_reference_war as (

    select * from {{ ref('stg_lahman__batting')}}

),

sums as (

    select
        batting.player_year_id
      , batting.person_id
      , batting.year_id
      , batting.is_postseason
      , count(distinct batting.team_id) as teams
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
    left join baseball_reference_war
        on batting.player_year_id = baseball_reference_war.player_year_id

    group by 1, 2, 3, 4

),

transformed as (

    select
        sums.player_year_id
      , sums.person_id
      , sums.year_id
      , sums.is_postseason
      , sums.teams
      , sums.games
      , sums.plate_appearances
      , sums.at_bats
      , sums.runs
      , sums.hits
      , sums.doubles
      , sums.triples
      , sums.home_runs
      , sums.times_on_base
      , sums.outs_made
      , sums.extra_base_hits
      , sums.total_bases
      , sums.runs_batted_in
      , sums.stolen_bases
      , sums.caught_stealing
      , sums.walks
      , sums.strikeouts
      , {{ no_divide_by_zero('sums.hits', 'sums.at_bats', 3) }} as batting_average
      , {{ no_divide_by_zero('sums.times_on_base', 'sums.on_base_denominator', 3) }} as on_base_percentage
      , {{ no_divide_by_zero('sums.total_bases', 'sums.at_bats', 3) }} as slugging_percentage
      , {{ no_divide_by_zero('sums.times_on_base', 'sums.on_base_denominator', 3) }} + {{ no_divide_by_zero('sums.total_bases', 'sums.at_bats', 3) }} as on_base_plus_slugging
      , sums.intentional_walks
      , sums.hit_by_pitches
      , sums.sacrifice_hits
      , sums.sacrifice_flies
      , sums.ground_into_double_plays
      , {{ no_divide_by_zero('sums.home_runs', 'sums.plate_appearances', 3) }} as home_run_rate
      , {{ no_divide_by_zero('sums.extra_base_hits', 'sums.plate_appearances', 3) }} as extra_base_hit_rate
      , {{ no_divide_by_zero('sums.strikeouts', 'sums.plate_appearances', 3) }} as strikeout_rate
      , {{ no_divide_by_zero('sums.walks', 'sums.plate_appearances', 3) }} as walk_rate
      , {{ no_divide_by_zero('sums.walks', 'sums.strikeouts', 3) }} as walk_per_strikeout
      , {{ no_divide_by_zero('sums.strikeouts', 'sums.walks', 3) }} as strikeout_per_walk

    from sums

)

select * from transformed
