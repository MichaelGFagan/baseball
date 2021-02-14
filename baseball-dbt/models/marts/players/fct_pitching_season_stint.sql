with pitching as (

    select * from {{ ref('stg_lahman__pitching') }}

),

teams as (

    select * from {{ ref('stg_lahman__teams') }}

),

transformed as (

    select
        pitching.person_id
      , pitching.year_id
      , pitching.stint
      , pitching.team_id
      , teams.franchise_id
      , pitching.league_id
      , pitching.wins
      , pitching.losses
      , pitching.games as games_pitched
      , pitching.games_started
      , pitching.games_finished
      , pitching.complete_games
      , pitching.shutouts
      , pitching.saves
      , pitching.batters_faced
      , pitching.outs_pitched
      , concat(cast(trunc(pitching.outs_pitched / 3) as string), '.', cast(mod(pitching.outs_pitched, 3) as string)) as innings_pitched
      , pitching.hits as hits_allowed
      , pitching.runs as runs_allowed
      , pitching.earned_runs as earned_runs_allowed
      , pitching.home_runs as home_runs_allowed
      , pitching.walks as walks_allowed
      , pitching.strikeouts as strikeouts_thrown
      , pitching.intentional_walks as intentional_walks_allowed
      , pitching.hit_by_pitches as hit_by_pitches_allowed
      , pitching.wild_pitches as wild_pitches_allowed
      , pitching.balks
      , pitching.sacrifice_hits as sacrifice_hits_allowed
      , pitching.sacrifice_flies as sacrifice_flies_allowed
      , pitching.ground_into_double_plays as ground_into_double_plays_induced
      , {{ no_divide_by_zero('pitching.hits', 'pitching.batters_faced - pitching.walks - pitching.hit_by_pitches - pitching.sacrifice_hits - pitching.sacrifice_flies', 3) }} as batting_average_against
      , {{ no_divide_by_zero('pitching.hits - pitching.home_runs', 'pitching.batters_faced - pitching.walks - pitching.hit_by_pitches - pitching.sacrifice_hits - pitching.strikeouts - pitching.home_runs', 3) }} as batting_average_on_balls_in_play
      , {{ no_divide_by_zero('pitching.runs', 'pitching.outs_pitched * 27', 2) }} as runs_allowed_average
      , {{ no_divide_by_zero('pitching.earned_runs', 'pitching.outs_pitched * 27', 2) }} as earned_runs_allowed_average
        -- FIP: (13 * pitching.home_runs + 3 * (walks + hit_by_pitches) - 2 * pitching.strikeouts) / pitching.batters_faced * 27 + constant as fielding_independent_pitching,
      , {{ no_divide_by_zero('pitching.walks + pitching.hits * 3', 'pitching.outs_pitched', 3) }} as whip
      , {{ no_divide_by_zero('pitching.hits', 'pitching.outs_pitched * 27', 1) }} as hits_allowed_per_nine
      , {{ no_divide_by_zero('pitching.home_runs', 'pitching.outs_pitched * 27', 1) }} as home_runs_allowed_per_nine
      , {{ no_divide_by_zero('pitching.walks', 'pitching.outs_pitched * 27', 1) }} as walks_allowed_per_nine
      , {{ no_divide_by_zero('pitching.walks', 'pitching.strikeouts', 2) }} as walks_allowed_per_strikeout_thrown
      , {{ no_divide_by_zero('pitching.strikeouts', 'pitching.batters_faced', 3) }} as strikeouts_thrown_rate
      , {{ no_divide_by_zero('pitching.walks', 'pitching.batters_faced', 3) }} as walks_allowed_rate
      , {{ no_divide_by_zero('pitching.home_runs', 'pitching.batters_faced', 3) }} as home_runs_allowed_rate


    from pitching
    left join teams
        on pitching.year_id = teams.year_id
        and pitching.team_id = teams.team_id

)

select * from transformed
