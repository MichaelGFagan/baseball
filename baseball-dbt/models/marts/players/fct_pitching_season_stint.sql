with pitching as (

    select * from {{ ref('stg_lahman__pitching') }}

),

teams as (

    select * from {{ ref('stg_lahman__teams') }}

),

transformed as (

    select
        p.person_id,
        p.year_id,
        p.stint,
        p.team_id,
        t.franchise_id,
        p.league_id,
        p.wins,
        p.losses,
        p.games as games_pitched,
        p.games_started,
        p.games_finished,
        p.complete_games,
        p.shutouts,
        p.saves,
        p.batters_faced,
        p.outs_pitched,
        concat(cast(trunc(p.outs_pitched / 3) as string), '.', cast(mod(p.outs_pitched, 3) as string)) as innings_pitched,
        p.hits as hits_allowed,
        p.runs as runs_allowed,
        p.earned_runs as earned_runs_allowed,
        p.home_runs as home_runs_allowed,
        p.walks as walks_allowed,
        p.strikeouts as strikeouts_thrown,
        p.intentional_walks as intentional_walks_allowed,
        p.hit_by_pitches as hit_by_pitches_allowed,
        p.wild_pitches as wild_pitches_allowed,
        p.balks,
        p.sacrifice_hits as sacrifice_hits_allowed,
        p.sacrifice_flies as sacrifice_flies_allowed,
        p.ground_into_double_plays as ground_into_double_plays_induced,
        {{ no_divide_by_zero('p.hits', 'p.batters_faced - p.walks - p.hit_by_pitches - p.sacrifice_hits - p.sacrifice_flies', 3) }} as batting_average_against,
        {{ no_divide_by_zero('p.hits - p.home_runs', 'p.batters_faced - p.walks - p.hit_by_pitches - p.sacrifice_hits - p.strikeouts - p.home_runs', 3) }} as batting_average_on_balls_in_play,
        {{ no_divide_by_zero('p.runs', 'p.outs_pitched * 27', 2) }} as runs_allowed_average,
        {{ no_divide_by_zero('p.earned_runs', 'p.outs_pitched * 27', 2) }} as earned_runs_allowed_average,
        -- FIP: (13 * p.home_runs + 3 * (walks + hit_by_pitches) - 2 * p.strikeouts) / p.batters_faced * 27 + constant as fielding_independent_pitching,
        {{ no_divide_by_zero('p.walks + p.hits * 3', 'p.outs_pitched', 3) }} as whip,
        {{ no_divide_by_zero('p.hits', 'p.outs_pitched * 27', 1) }} as hits_allowed_per_nine,
        {{ no_divide_by_zero('p.home_runs', 'p.outs_pitched * 27', 1) }} as home_runs_allowed_per_nine,
        {{ no_divide_by_zero('p.walks', 'p.outs_pitched * 27', 1) }} as walks_allowed_per_nine,
        {{ no_divide_by_zero('p.walks', 'p.strikeouts', 2) }} as walks_allowed_per_strikeout_thrown,
        {{ no_divide_by_zero('p.strikeouts', 'p.batters_faced', 3) }} as strikeouts_thrown_rate,
        {{ no_divide_by_zero('p.walks', 'p.batters_faced', 3) }} as walks_allowed_rate,
        {{ no_divide_by_zero('p.home_runs', 'p.batters_faced', 3) }} as home_runs_allowed_rate


    from pitching as p
    left join teams as t using (year_id, team_id)

)

select * from transformed
