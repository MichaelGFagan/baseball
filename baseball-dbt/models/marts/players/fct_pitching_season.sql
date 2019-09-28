with pitching as (

    select * from {{ ref('stg_lahman__pitching') }}

),

sums as (

    select
        p.person_id,
        p.year_id,
        p.is_postseason,
        count(distinct p.team_id) as teams,
        sum(ifnull(p.wins, 0)) as wins,
        sum(ifnull(p.losses, 0)) as losses,
        sum(ifnull(p.games, 0)) as games,
        sum(ifnull(p.games_started, 0)) as games_started,
        sum(ifnull(p.games_finished, 0)) as games_finished,
        sum(ifnull(p.complete_games, 0)) as complete_games,
        sum(ifnull(p.shutouts, 0)) as shutouts,
        sum(ifnull(p.saves, 0)) as saves,
        sum(ifnull(p.batters_faced, 0)) as batters_faced,
        sum(ifnull(p.outs_pitched, 0)) as outs_pitched,
        sum(ifnull(p.hits, 0)) as hits,
        sum(ifnull(p.runs, 0)) as runs,
        sum(ifnull(p.earned_runs, 0)) as earned_runs,
        sum(ifnull(p.home_runs, 0)) as home_runs,
        sum(ifnull(p.walks, 0)) as walks,
        sum(ifnull(p.strikeouts, 0)) as strikeouts,
        sum(ifnull(p.intentional_walks, 0)) as intentional_walks,
        sum(ifnull(p.hit_by_pitch, 0)) as hit_by_pitch,
        sum(ifnull(p.wild_pitches, 0)) as wild_pitches,
        sum(ifnull(p.balks, 0)) as balks,
        sum(ifnull(p.sacrifice_hits, 0)) as sacrifice_hits,
        sum(ifnull(p.sacrifice_flies, 0)) as sacrifice_flies,
        sum(ifnull(p.ground_into_double_plays, 0)) as ground_into_double_plays

    from pitching as p

    group by 1, 2, 3

)

select
    s.person_id,
    s.year_id,
    s.is_postseason,
    s.teams,
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
    case
        when (s.batters_faced - s.walks - s.hit_by_pitch - s.sacrifice_hits - s.sacrifice_flies) = 0 then 0
        else round(s.hits / (s.batters_faced - s.walks - s.hit_by_pitch - s.sacrifice_hits - s.sacrifice_flies), 3)
    end as batting_average_against,
    case
        when (s.batters_faced - s.walks - s.hit_by_pitch - s.sacrifice_hits - s.strikeouts - s.home_runs) = 0 then 0
        else round((s.hits - s.home_runs) / (s.batters_faced - s.walks - s.hit_by_pitch - s.sacrifice_hits - s.strikeouts - s.home_runs), 3)
    end as batting_average_on_balls_in_play,
    case
        when s.outs_pitched = 0 then 0
        else round(s.runs / s.outs_pitched * 27, 2)
    end as run_average,
    case
        when s.outs_pitched = 0 then 0
        else round(s.earned_runs / s.outs_pitched * 27, 2)
    end as earned_run_average,
    -- FIP: (13 * s.home_runs + 3 * (walks + hit_by_pitch) - 2 * s.strikeouts) / s.batters_faced * 27 + constant as fielding_independent_pitching,
    case
        when s.outs_pitched = 0 then 0
        else round((s.walks + s.hits) * 3 / s.outs_pitched, 3)
    end as whip,
    case
        when s.outs_pitched = 0 then 0
        else round(s.hits / s.outs_pitched * 27, 1)
    end as hits_per_nine,
    case
        when s.outs_pitched = 0 then 0
        else round(s.home_runs / s.outs_pitched * 27, 1)
    end as home_runs_per_nine,
    case
        when s.outs_pitched = 0 then 0
        else round(s.walks / s.outs_pitched * 27, 1)
    end as walks_per_nine,
    case
        when s.strikeouts = 0 then 0
        else round(s.walks / s.strikeouts, 2)
    end as walks_per_strikeout,
    case
        when s.batters_faced = 0 then 0
        else round(s.strikeouts / s.batters_faced, 3)
    end as strikeout_rate,
    case
        when s.batters_faced = 0 then 0
        else round(s.walks / s.batters_faced, 3)
    end as walk_rate,
    case
        when s.batters_faced = 0 then 0
        else round(s.home_runs / s.batters_faced, 3)
    end as home_run_rate

from sums as s
