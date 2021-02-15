with pitching as (

    select * from {{ ref('stg_lahman__pitching') }}

),

sums as (

    select
        pitching.person_id
      , pitching.year_id
      , pitching.is_postseason
      , count(distinct pitching.team_id) as teams
      , sum(ifnull(pitching.wins, 0)) as wins
      , sum(ifnull(pitching.losses, 0)) as losses
      , sum(ifnull(pitching.games, 0)) as games
      , sum(ifnull(pitching.games_started, 0)) as games_started
      , sum(ifnull(pitching.games_finished, 0)) as games_finished
      , sum(ifnull(pitching.complete_games, 0)) as complete_games
      , sum(ifnull(pitching.shutouts, 0)) as shutouts
      , sum(ifnull(pitching.saves, 0)) as saves
      , sum(ifnull(pitching.batters_faced, 0)) as batters_faced
      , sum(ifnull(pitching.outs_pitched, 0)) as outs_pitched
      , sum(ifnull(pitching.hits, 0)) as hits
      , sum(ifnull(pitching.runs, 0)) as runs
      , sum(ifnull(pitching.earned_runs, 0)) as earned_runs
      , sum(ifnull(pitching.home_runs, 0)) as home_runs
      , sum(ifnull(pitching.walks, 0)) as walks
      , sum(ifnull(pitching.strikeouts, 0)) as strikeouts
      , sum(ifnull(pitching.intentional_walks, 0)) as intentional_walks
      , sum(ifnull(pitching.hit_by_pitches, 0)) as hit_by_pitches
      , sum(ifnull(pitching.wild_pitches, 0)) as wild_pitches
      , sum(ifnull(pitching.balks, 0)) as balks
      , sum(ifnull(pitching.sacrifice_hits, 0)) as sacrifice_hits
      , sum(ifnull(pitching.sacrifice_flies, 0)) as sacrifice_flies
      , sum(ifnull(pitching.ground_into_double_plays, 0)) as ground_into_double_plays

    from pitching

    group by 1, 2, 3

),

transformed as (

    select
        sums.person_id
      , sums.year_id
      , sums.is_postseason
      , sums.teams
      , sums.wins
      , sums.losses
      , sums.games
      , sums.games_started
      , sums.games_finished
      , sums.complete_games
      , sums.shutouts
      , sums.saves
      , sums.batters_faced
      , sums.outs_pitched
      , concat(cast(trunc(sums.outs_pitched / 3) as string), '.', cast(mod(sums.outs_pitched, 3) as string)) as innings_pitched
      , sums.hits as hits_allowed
      , sums.runs as runs_allowed
      , sums.earned_runs as earned_runs_allowed
      , sums.home_runs as home_runs_allowed
      , sums.walks as walks_allowed
      , sums.strikeouts as strikeouts_thrown
      , sums.intentional_walks as intentional_walks_allowed
      , sums.hit_by_pitches as hit_by_pitches_allowed
      , sums.wild_pitches
      , sums.balks
      , sums.sacrifice_hits as sacrifice_hits_allowed
      , sums.sacrifice_flies as sacrifice_flies_allowed
      , sums.ground_into_double_plays as ground_into_double_plays_induced
      , {{ no_divide_by_zero('sums.hits', 'sums.batters_faced - sums.walks - sums.hit_by_pitches - sums.sacrifice_hits - sums.sacrifice_flies', 3) }} as batting_average_against
      , {{ no_divide_by_zero('sums.hits - sums.home_runs', 'sums.batters_faced - sums.walks - sums.hit_by_pitches - sums.sacrifice_hits - sums.strikeouts - sums.home_runs', 3) }} as batting_average_on_balls_in_play
      , {{ no_divide_by_zero('sums.runs', 'sums.outs_pitched * 27', 2) }} as runs_allowed_average
      , {{ no_divide_by_zero('sums.earned_runs', 'sums.outs_pitched * 27', 2) }} as earned_runs_allowed_average
        -- FIP: (13 * sums.home_runs + 3 * (walks + hit_by_pitches) - 2 * sums.strikeouts) / sums.batters_faced * 27 + constant as fielding_independent_pitching,
      , {{ no_divide_by_zero('sums.walks + sums.hits * 3', 'sums.outs_pitched', 3) }} as whip
      , {{ no_divide_by_zero('sums.hits', 'sums.outs_pitched * 27', 1) }} as hits_allowed_per_nine
      , {{ no_divide_by_zero('sums.home_runs', 'sums.outs_pitched * 27', 1) }} as home_runs_allowed_per_nine
      , {{ no_divide_by_zero('sums.walks', 'sums.outs_pitched * 27', 1) }} as walks_allowed_per_nine
      , {{ no_divide_by_zero('sums.walks', 'sums.strikeouts', 2) }} as walks_allowed_per_strikeout_thrown
      , {{ no_divide_by_zero('sums.strikeouts', 'sums.batters_faced', 3) }} as strikeouts_thrown_rate
      , {{ no_divide_by_zero('sums.walks', 'sums.batters_faced', 3) }} as walks_allowed_rate
      , {{ no_divide_by_zero('sums.home_runs', 'sums.batters_faced', 3) }} as home_runs_allowed_rate

    from sums

)

select * from transformed
