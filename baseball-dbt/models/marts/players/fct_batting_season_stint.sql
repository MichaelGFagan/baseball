with batting as (

    select * from {{ ref('stg_lahman__batting') }}

),

transformed as (

    select
        batting.person_id
      , batting.year_id
      , batting.stint
      , batting.team_id
      , teams.franchise_id
      , batting.league_id
      , batting.games
      , batting.plate_appearances
      , batting.at_bats
      , batting.runs
      , batting.hits
      , batting.doubles
      , batting.triples
      , batting.home_runs
      , batting.times_on_base
      , batting.outs_made
      , batting.extra_base_hits
      , batting.total_bases
      , batting.runs_batted_in
      , batting.stolen_bases
      , batting.caught_stealing
      , batting.walks
      , batting.strikeouts
      , {{ no_divide_by_zero('batting.hits', 'batting.at_bats', 3) }} as batting_average
      , {{ no_divide_by_zero('batting.times_on_base', 'batting.on_base_denominator', 3) }} as on_base_percentage
      , {{ no_divide_by_zero('batting.total_bases', 'batting.at_bats', 3) }} as slugging_percentage
      , {{ no_divide_by_zero('batting.times_on_base', 'batting.on_base_denominator', 3) }} + {{ no_divide_by_zero('batting.total_bases', 'batting.at_bats', 3) }} as on_base_plus_slugging
      , batting.intentional_walks
      , batting.hit_by_pitches
      , batting.sacrifice_hits
      , batting.sacrifice_flies
      , batting.ground_into_double_plays
      , {{ no_divide_by_zero('batting.home_runs', 'batting.plate_appearances', 3) }} as home_run_rate
      , {{ no_divide_by_zero('batting.extra_base_hits', 'batting.plate_appearances', 3) }} as extra_base_hit_rate
      , {{ no_divide_by_zero('batting.strikeouts', 'batting.plate_appearances', 3) }} as strikeout_rate
      , {{ no_divide_by_zero('batting.walks', 'batting.plate_appearances', 3) }} as walk_rate
      , {{ no_divide_by_zero('batting.walks', 'batting.strikeouts', 3) }} as walk_per_strikeout
      , {{ no_divide_by_zero('batting.strikeouts', 'batting.walks', 3) }} as strikeout_per_walk

    from batting as b

)

select * from transformed
