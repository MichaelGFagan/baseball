with batting as (

    select * from {{ ref('stg_lahman__batting') }}

),

sums as (

    select
        batting.person_id
      , batting.year_id
      , batting.is_postseason
      , count(distinct batting.team_id) as teams
      , sum(ifnull(batting.games, 0)) as games
      , sum(ifnull(batting.plate_appearances, 0)) as plate_appearances
      , sum(ifnull(batting.at_bats, 0)) as at_bats
      , sum(ifnull(batting.runs, 0)) as runs
      , sum(ifnull(batting.hits, 0)) as hits
      , sum(ifnull(batting.doubles, 0)) as doubles
      , sum(ifnull(batting.triples, 0)) as triples
      , sum(ifnull(batting.home_runs, 0)) as home_runs
      , sum(ifnull(batting.times_on_base, 0)) as times_on_base
      , sum(ifnull(batting.outs_made, 0)) as outs_made
      , sum(ifnull(batting.extra_base_hits, 0)) as extra_base_hits
      , sum(ifnull(batting.total_bases, 0)) as total_bases
      , sum(ifnull(batting.runs_batted_in, 0)) as runs_batted_in
      , sum(ifnull(batting.stolen_bases, 0)) as stolen_bases
      , sum(ifnull(batting.caught_stealing, 0)) as caught_stealing
      , sum(ifnull(batting.walks, 0)) as walks
      , sum(ifnull(batting.strikeouts, 0)) as strikeouts
      , sum(ifnull(batting.intentional_walks, 0)) as intentional_walks
      , sum(ifnull(batting.hit_by_pitches, 0)) as hit_by_pitches
      , sum(ifnull(batting.sacrifice_hits, 0)) as sacrifice_hits
      , sum(ifnull(batting.sacrifice_flies, 0)) as sacrifice_flies
      , sum(ifnull(batting.ground_into_double_plays, 0)) as ground_into_double_plays
      , sum(ifnull(batting.on_base_denominator, 0)) as on_base_denominator

    from batting

    group by 1, 2, 3

),

transformed as (

    select
        sums.person_id
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
