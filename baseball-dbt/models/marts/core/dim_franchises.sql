with teams as (

    select * from {{ ref('stg_lahman__teams') }}

),

franchises as (

    select * from {{ ref('stg_lahman__franchises') }}

),

sum as (

    select
        f.franchise_id,
        f.franchise_name,
        f.is_active,
        sum(t.attendance) as attendance,
        sum(t.games) as games,
        sum(t.wins) as wins,
        sum(t.losses) as losses,
        round(sum(wins) / sum(games), 3) as winning_percentage,
        sum(is_division_winner, 1, 0) as times_division_winner,
        sum(is_wild_card_winner, 1, 0) as times_wild_card_winner,
        sum(is_league_champion, 1, 0) as times_league_champion,
        sum(is_world_series_champion, 1, 0) as times_world_series_champion,
        sum(ifnull(runs_scored, 0)) as runs_scored,
        sum(ifnull(hits, 0)) as hits,
        sum(ifnull(doubles, 0)) as doubles,
        sum(ifnull(triples, 0)) as triples,
        sum(ifnull(home_runs, 0)) as home_runs,
        sum(ifnull(walks, 0)) as walks,
        sum(ifnull(strikeouts, 0)) as strikeouts,
        sum(ifnull(stolen_bases, 0)) as stolen_bases,
        sum(ifnull(caught_stealing, 0)) as caught_stealing,
        sum(ifnull(hit_by_pitch, 0)) as hit_by_pitch,
        sum(ifnull(sacrifice_flies, 0)) as sacrifice_flies,
        sum(ifnull(runs_allowed, 0)) as runs_allowed,
        sum(ifnull(earned_runs_allowed, 0)) as earned_runs_allowed,
        sum(ifnull(complete_games, 0)) as complete_games,
        sum(ifnull(shutouts, 0)) as shutouts,
        sum(ifnull(saves, 0)) as saves,
        sum(ifnull(outs_pitched, 0)) as outs_pitched,
        sum(ifnull(hits_allowed, 0)) as hits_allowed,
        sum(ifnull(home_runs_allowed, 0)) as home_runs_allowed,
        sum(ifnull(walks_allowed, 0)) as walks_allowed,
        sum(ifnull(strikeouts_pitched, 0)) as strikeouts_pitched,
        sum(ifnull(errors, 0)) as errors,
        sum(ifnull(double_plays, 0)) as double_plays,


)
