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
        p.games,
        p.games_started,
        p.games_finished,
        p.complete_games,
        p.shutouts,
        p.saves,
        p.batters_faced,
        p.outs_pitched,
        concat(cast(trunc(p.outs_pitched / 3) as string), '.', cast(mod(p.outs_pitched, 3) as string)) as innings_pitched,
        p.hits,
        p.runs,
        p.earned_runs,
        p.home_runs,
        p.walks,
        p.strikeouts,
        p.intentional_walks,
        p.hit_by_pitch,
        p.wild_pitches,
        p.balks,
        p.sacrifice_hits,
        p.sacrifice_flies,
        p.ground_into_double_plays,
        {{ no_divide_by_zero('p.hits', 'p.batters_faced - p.walks - p.hit_by_pitch - p.sacrifice_hits - p.sacrifice_flies', 3) }} as batting_average_against,
        {{ no_divide_by_zero('p.hits - p.home_runs', 'p.batters_faced - p.walks - p.hit_by_pitch - p.sacrifice_hits - p.strikeouts - p.home_runs', 3) }} as batting_average_on_balls_in_play,
        {{ no_divide_by_zero('p.runs', 'p.outs_pitched * 27', 2) }} as run_average,
        {{ no_divide_by_zero('p.earned_runs', 'p.outs_pitched * 27', 2) }} as earned_run_average,
        -- FIP: (13 * p.home_runs + 3 * (walks + hit_by_pitch) - 2 * p.strikeouts) / p.batters_faced * 27 + constant as fielding_independent_pitching,
        {{ no_divide_by_zero('p.walks + p.hits * 3', 'p.outs_pitched', 3) }} as whip,
        {{ no_divide_by_zero('p.hits', 'p.outs_pitched * 27', 1) }} as hits_per_nine,
        {{ no_divide_by_zero('p.home_runs', 'p.outs_pitched * 27', 1) }} as home_runs_per_nine,
        {{ no_divide_by_zero('p.walks', 'p.outs_pitched * 27', 1) }} as walks_per_nine,
        {{ no_divide_by_zero('p.walks', 'p.strikeouts', 2) }} as walks_per_strikeout,
        {{ no_divide_by_zero('p.strikeouts', 'p.batters_faced', 3) }} as strikeout_rate,
        {{ no_divide_by_zero('p.walks', 'p.batters_faced', 3) }} as walk_rate,
        {{ no_divide_by_zero('p.home_runs', 'p.batters_faced', 3) }} as home_run_rate


    from pitching as p
    left join teams as t using (year_id, team_id)

)

select * from transformed
