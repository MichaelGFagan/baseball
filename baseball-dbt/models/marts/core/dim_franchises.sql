with teams as (

    select * from {{ ref('stg_lahman__teams') }}

),

franchises as (

    select * from {{ ref('stg_lahman__franchises') }}

),

sums as (

    select
        f.franchise_id,
        f.franchise_name,
        f.is_active,
        --TODO:
        --sum(team names)
        --sum(ballparks)
        --sum(players)
        --sum(hall_of_fame)
        --sum(playoffs)
        count(t.year_id) as seasons,
        min(t.year_id) as inaugural_year,
        max(t.year_id) as latest_year,
        sum(ifnull(t.attendance, 0)) as attendance,
        sum(case when t.attendance is not null then 1 else 0 end) as years_with_attendance,
        sum(t.games) as games,
        sum(t.wins) as wins,
        sum(t.losses) as losses,
        round(sum(wins) / sum(games), 3) as winning_percentage,
        sum(case when t.is_division_winner then 1 else 0 end) as times_division_winner,
        sum(case when t.is_wild_card_winner then 1 else 0 end) as times_wild_card_winner,
        sum(case when t.is_league_champion then 1 else 0 end) as times_league_champion,
        sum(case when t.is_world_series_champion then 1 else 0 end) as times_world_series_champion,
        sum(ifnull(t.runs_scored, 0)) as runs_scored,
        sum(ifnull(t.hits, 0)) as hits,
        sum(ifnull(t.doubles, 0)) as doubles,
        sum(ifnull(t.triples, 0)) as triples,
        sum(ifnull(t.home_runs, 0)) as home_runs,
        sum(ifnull(t.walks, 0)) as walks,
        sum(ifnull(t.strikeouts, 0)) as strikeouts,
        sum(ifnull(t.stolen_bases, 0)) as stolen_bases,
        sum(ifnull(t.caught_stealing, 0)) as caught_stealing,
        sum(ifnull(t.hit_by_pitch, 0)) as hit_by_pitch,
        sum(ifnull(t.sacrifice_flies, 0)) as sacrifice_flies,
        sum(ifnull(t.runs_allowed, 0)) as runs_allowed,
        sum(ifnull(t.earned_runs_allowed, 0)) as earned_runs_allowed,
        sum(ifnull(t.complete_games, 0)) as complete_games,
        sum(ifnull(t.shutouts, 0)) as shutouts,
        sum(ifnull(t.saves, 0)) as saves,
        sum(ifnull(t.outs_pitched, 0)) as outs_pitched,
        sum(ifnull(t.hits_allowed, 0)) as hits_allowed,
        sum(ifnull(t.home_runs_allowed, 0)) as home_runs_allowed,
        sum(ifnull(t.walks_allowed, 0)) as walks_allowed,
        sum(ifnull(t.strikeouts_pitched, 0)) as strikeouts_pitched,
        sum(ifnull(t.errors, 0)) as errors,
        sum(ifnull(t.double_plays, 0)) as double_plays

    from franchises as f
    join teams as t using (franchise_id)

    group by 1, 2, 3

),

transformed as (

    select
        s.franchise_id,
        s.franchise_name,
        s.is_active,
        s.seasons,
        s.inaugural_year,
        s.latest_year,
        s.attendance,
        case
            when s.years_with_attendance = 0 then 0
            else round(s.attendance / s.years_with_attendance)
        end as average_attendance,
        s.games,
        s.wins,
        s.losses,
        round(s.wins / s.games, 3) as win_loss_percentage,
        s.times_wild_card_winner,
        s.times_division_winner,
        s.times_league_champion,
        s.times_world_series_champion,
        s.runs_scored,
        s.hits,
        s.doubles,
        s.triples,
        s.home_runs,
        s.walks,
        s.strikeouts,
        s.stolen_bases,
        s.caught_stealing,
        case
            when s.stolen_bases + s.caught_stealing = 0 then 0
            else s.stolen_bases / (s.stolen_bases + s.caught_stealing)
        end as stolen_base_percentage,
        s.hit_by_pitch,
        s.sacrifice_flies,
        s.runs_allowed,
        round(s.runs_allowed / s.outs_pitched * 27, 2) as run_average,
        s.earned_runs_allowed,
        round(s.earned_runs_allowed / s.outs_pitched * 27, 2) as earned_run_average,
        s.complete_games,
        s.shutouts,
        s.saves,
        s.outs_pitched,
        s.hits_allowed,
        s.home_runs_allowed,
        s.walks_allowed,
        s.strikeouts_pitched,
        s.errors,
        s.double_plays

    from sums as s

)

select * from transformed
