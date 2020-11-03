with pitching as (

    select * from {{ ref('stg_lahman__pitching') }} where is_postseason = TRUE

),

teams as (

    select * from {{ ref('stg_lahman__teams') }}

),

sums as (

    select
        p.person_id,
        count(distinct p.year_id) as seasons,
        count(distinct t.franchise_id) as franchises,
        {{ all_null_or_sum('p.wins') }} as wins,
        {{ all_null_or_sum('p.losses') }} as losses,
        {{ all_null_or_sum('p.games') }} as games_pitched,
        {{ all_null_or_sum('p.games_started') }} as games_started,
        {{ all_null_or_sum('p.games_finished') }} as games_finished,
        {{ all_null_or_sum('p.complete_games') }} as complete_games,
        {{ all_null_or_sum('p.shutouts') }} as shutouts,
        {{ all_null_or_sum('p.saves') }} as saves,
        {{ all_null_or_sum('p.batters_faced') }} as batters_faced,
        {{ all_null_or_sum('p.outs_pitched') }} as outs_pitched,
        {{ all_null_or_sum('p.hits') }} as hits,
        {{ all_null_or_sum('p.runs') }} as runs,
        {{ all_null_or_sum('p.earned_runs') }} as earned_runs,
        {{ all_null_or_sum('p.home_runs') }} as home_runs,
        {{ all_null_or_sum('p.walks') }} as walks,
        {{ all_null_or_sum('p.strikeouts') }} as strikeouts,
        {{ all_null_or_sum('p.intentional_walks') }} as intentional_walks,
        {{ all_null_or_sum('p.hit_by_pitch') }} as hit_by_pitch,
        {{ all_null_or_sum('p.wild_pitches') }} as wild_pitches,
        {{ all_null_or_sum('p.balks') }} as balks,
        {{ all_null_or_sum('p.sacrifice_hits') }} as sacrifice_hits,
        {{ all_null_or_sum('p.sacrifice_flies') }} as sacrifice_flies,
        {{ all_null_or_sum('p.ground_into_double_plays') }} as ground_into_double_plays

    from pitching as p
    join teams as t using (year_id, team_id)

    group by 1

),

transformed as (

    select
        s.person_id,
        s.seasons,
        s.franchises,
        s.wins,
        s.losses,
        s.games,
        s.games_started,
        s.games_finished,
        s.complete_games,
        s.shutouts,
        s.saves,
        s.batters_faced,
        s.outs_pitched,
        concat(cast(trunc(s.outs_pitched / 3) as string), '.', cast(mod(s.outs_pitched, 3) as string)) as innings_pitched,
        s.hits,
        s.runs,
        s.earned_runs,
        s.home_runs,
        s.walks,
        s.strikeouts,
        s.intentional_walks,
        s.hit_by_pitch,
        s.wild_pitches,
        s.balks,
        s.sacrifice_hits,
        s.sacrifice_flies,
        s.ground_into_double_plays,
        {{ no_divide_by_zero('s.hits', 's.batters_faced - s.walks - s.hit_by_pitch - s.sacrifice_hits - s.sacrifice_flies', 3) }} as batting_average_against,
        {{ no_divide_by_zero('s.hits - s.home_runs', 's.batters_faced - s.walks - s.hit_by_pitch - s.sacrifice_hits - s.strikeouts - s.home_runs', 3) }} as batting_average_on_balls_in_play,
        {{ no_divide_by_zero('s.runs', 's.outs_pitched * 27', 2) }} as run_average,
        {{ no_divide_by_zero('s.earned_runs', 's.outs_pitched * 27', 2) }} as earned_run_average,
        -- FIP: (13 * s.home_runs + 3 * (walks + hit_by_pitch) - 2 * s.strikeouts) / s.batters_faced * 27 + constant as fielding_independent_pitching,
        {{ no_divide_by_zero('s.walks + s.hits * 3', 's.outs_pitched', 3) }} as whip,
        {{ no_divide_by_zero('s.hits', 's.outs_pitched * 27', 1) }} as hits_per_nine,
        {{ no_divide_by_zero('s.home_runs', 's.outs_pitched * 27', 1) }} as home_runs_per_nine,
        {{ no_divide_by_zero('s.walks', 's.outs_pitched * 27', 1) }} as walks_per_nine,
        {{ no_divide_by_zero('s.walks', 's.strikeouts', 2) }} as walks_per_strikeout,
        {{ no_divide_by_zero('s.strikeouts', 's.batters_faced', 3) }} as strikeout_rate,
        {{ no_divide_by_zero('s.walks', 's.batters_faced', 3) }} as walk_rate,
        {{ no_divide_by_zero('s.home_runs', 's.batters_faced', 3) }} as home_run_rate

    from sums as s

)

select * from transformed
