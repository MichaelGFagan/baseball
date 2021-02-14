with pitching as (

    select * from {{ ref('stg_lahman__pitching') }} where is_postseason = TRUE

),

teams as (

    select * from {{ ref('stg_lahman__teams') }}

),

sums as (

    select
        pitching.person_id
        {# count(distinct pitching.year_id) as seasons,
        count(distinct t.franchise_id) as franchises, #}
      , {{ all_null_or_sum('pitching.wins') }} as wins
      , {{ all_null_or_sum('pitching.losses') }} as losses
      , {{ all_null_or_sum('pitching.games') }} as games_pitched
      , {{ all_null_or_sum('pitching.games_started') }} as games_started
      , {{ all_null_or_sum('pitching.games_finished') }} as games_finished
      , {{ all_null_or_sum('pitching.complete_games') }} as complete_games
      , {{ all_null_or_sum('pitching.shutouts') }} as shutouts
      , {{ all_null_or_sum('pitching.saves') }} as saves
      , {{ all_null_or_sum('pitching.batters_faced') }} as batters_faced
      , {{ all_null_or_sum('pitching.outs_pitched') }} as outs_pitched
      , {{ all_null_or_sum('pitching.hits') }} as hits
      , {{ all_null_or_sum('pitching.runs') }} as runs
      , {{ all_null_or_sum('pitching.earned_runs') }} as earned_runs
      , {{ all_null_or_sum('pitching.home_runs') }} as home_runs
      , {{ all_null_or_sum('pitching.walks') }} as walks
      , {{ all_null_or_sum('pitching.strikeouts') }} as strikeouts
      , {{ all_null_or_sum('pitching.intentional_walks') }} as intentional_walks
      , {{ all_null_or_sum('pitching.hit_by_pitches') }} as hit_by_pitches
      , {{ all_null_or_sum('pitching.wild_pitches') }} as wild_pitches
      , {{ all_null_or_sum('pitching.balks') }} as balks
      , {{ all_null_or_sum('pitching.sacrifice_hits') }} as sacrifice_hits
      , {{ all_null_or_sum('pitching.sacrifice_flies') }} as sacrifice_flies
      , {{ all_null_or_sum('pitching.ground_into_double_plays') }} as ground_into_double_plays

    from pitching as p

    group by 1

),

transformed as (

    select
        sums.person_id
      , sums.wins as postseason_wins
      , sums.losses as postseason_losses
      , sums.games_pitched as postseason_games_as_pitcher
      , sums.games_started as postseason_games_as_starting_pitcher
      , sums.games_finished as postseason_games_as_finishing_pitcher
      , sums.complete_games as postseason_complete_games
      , sums.shutouts as postseason_shutouts
      , sums.saves as postseason_saves
      , sums.batters_faced as postseason_batters_faced
      , sums.outs_pitched as postseason_outs_pitched
      , concat(cast(trunc(sums.outs_pitched / 3) as string), '.', cast(mod(sums.outs_pitched, 3) as string)) as postseason_innings_pitched
      , sums.hits as postseason_hits_allowed
      , sums.runs as postseason_runs_allowed
      , sums.earned_runs as postseason_earned_runs_allowed
      , sums.home_runs as postseason_home_runs_allowed
      , sums.walks as postseason_walks_allowed
      , sums.strikeouts as postseason_strikeouts_thrown
      , sums.intentional_walks as postseason_intentional_walks_allowed
      , sums.hit_by_pitches as postseason_hit_by_pitches_thrown
      , sums.wild_pitches as postseason_wild_pitches
      , sums.balks as postseason_balks
      , sums.sacrifice_hits as postseason_sacrifice_hits_allowed
      , sums.sacrifice_flies as postseason_sacrifice_flies_allowed
      , sums.ground_into_double_plays as postseason_ground_into_double_plays_induced
      , {{ no_divide_by_zero('sums.hits', 'sums.batters_faced - sums.walks - sums.hit_by_pitches - sums.sacrifice_hits - sums.sacrifice_flies', 3) }} as postseason_batting_average_against
      , {{ no_divide_by_zero('sums.hits - sums.home_runs', 'sums.batters_faced - sums.walks - sums.hit_by_pitches - sums.sacrifice_hits - sums.strikeouts - sums.home_runs', 3) }} as postseason_batting_average_on_balls_in_play
      , {{ no_divide_by_zero('sums.runs', 'sums.outs_pitched * 27', 2) }} as postseason_runs_allowed_average
      , {{ no_divide_by_zero('sums.earned_runs', 'sums.outs_pitched * 27', 2) }} as postseason_earned_runs_allowed_average
        -- FIP: (13 * sums.home_runs + 3 * (walks + hit_by_pitches) - 2 * sums.strikeouts) / sums.batters_faced * 27 + constant as fielding_independent_pitching,
      , {{ no_divide_by_zero('sums.walks + sums.hits * 3', 'sums.outs_pitched', 3) }} as postseason_whip
      , {{ no_divide_by_zero('sums.hits', 'sums.outs_pitched * 27', 1) }} as postseason_hits_allowed_per_nine
      , {{ no_divide_by_zero('sums.home_runs', 'sums.outs_pitched * 27', 1) }} as postseason_home_runs_allowed_per_nine
      , {{ no_divide_by_zero('sums.walks', 'sums.outs_pitched * 27', 1) }} as postseason_walks_allowed_per_nine
      , {{ no_divide_by_zero('sums.walks', 'sums.strikeouts', 2) }} as postseason_walks_allowed_per_strikeout_thrown
      , {{ no_divide_by_zero('sums.strikeouts', 'sums.batters_faced', 3) }} as postseason_strikeouts_thrown_rate
      , {{ no_divide_by_zero('sums.walks', 'sums.batters_faced', 3) }} as postseason_walks_allowed_rate
      , {{ no_divide_by_zero('sums.home_runs', 'sums.batters_faced', 3) }} as postseason_home_runs_allowed_rate

    from sums

)

select * from transformed
