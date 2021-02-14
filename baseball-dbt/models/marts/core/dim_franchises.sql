with teams as (

    select * from {{ ref('stg_lahman__teams') }}

),

franchises as (

    select * from {{ ref('stg_lahman__franchises') }}

),

sums as (

    select
        franchises.franchise_id
      , franchises.franchise_name
      , franchises.is_active
        --TODO:
        --sum(team names)
        --sum(ballparks)
        --sum(players)
        --sum(hall_of_fame)
        --sum(playoffs)
      , count(teams.year_id) as seasons
      , min(teams.year_id) as inaugural_year
      , max(teams.year_id) as latest_year
      , sum(ifnull(teams.attendance, 0)) as attendance
      , sum(case when teams.attendance is not null then 1 else 0 end) as years_with_attendance
      , sum(teams.games) as games
      , sum(teams.wins) as wins
      , sum(teams.losses) as losses
      , round(sum(wins) / sum(games), 3) as winning_percentage
      , sum(case when teams.is_division_winner then 1 else 0 end) as times_division_winner
      , sum(case when teams.is_wild_card_winner then 1 else 0 end) as times_wild_card_winner
      , sum(case when teams.is_league_champion then 1 else 0 end) as times_league_champion
      , sum(case when teams.is_world_series_champion then 1 else 0 end) as times_world_series_champion
      , sum(ifnull(teams.runs_scored, 0)) as runs_scored
      , sum(ifnull(teams.hits, 0)) as hits
      , sum(ifnull(teams.doubles, 0)) as doubles
      , sum(ifnull(teams.triples, 0)) as triples
      , sum(ifnull(teams.home_runs, 0)) as home_runs
      , sum(ifnull(teams.walks, 0)) as walks
      , sum(ifnull(teams.strikeouts, 0)) as strikeouts
      , sum(ifnull(teams.stolen_bases, 0)) as stolen_bases
      , sum(ifnull(teams.caught_stealing, 0)) as caught_stealing
      , sum(ifnull(teams.hit_by_pitch, 0)) as hit_by_pitch
      , sum(ifnull(teams.sacrifice_flies, 0)) as sacrifice_flies
      , sum(ifnull(teams.runs_allowed, 0)) as runs_allowed
      , sum(ifnull(teams.earned_runs_allowed, 0)) as earned_runs_allowed
      , sum(ifnull(teams.complete_games, 0)) as complete_games
      , sum(ifnull(teams.shutouts, 0)) as shutouts
      , sum(ifnull(teams.saves, 0)) as saves
      , sum(ifnull(teams.outs_pitched, 0)) as outs_pitched
      , sum(ifnull(teams.hits_allowed, 0)) as hits_allowed
      , sum(ifnull(teams.home_runs_allowed, 0)) as home_runs_allowed
      , sum(ifnull(teams.walks_allowed, 0)) as walks_allowed
      , sum(ifnull(teams.strikeouts_pitched, 0)) as strikeouts_pitched
      , sum(ifnull(teams.errors, 0)) as errors
      , sum(ifnull(teams.double_plays, 0)) as double_plays

    from franchises
    join teams
        on franchises.franchise_id = teams.franchise_id

    group by 1, 2, 3

),

transformed as (

    select
        sums.franchise_id
      , sums.franchise_name
      , sums.is_active
      , sums.seasons
      , sums.inaugural_year
      , sums.latest_year
      , sums.attendance
      , case
            when sums.years_with_attendance = 0 then 0
            else round(sums.attendance / sums.years_with_attendance)
        end as average_attendance
      , sums.games
      , sums.wins
      , sums.losses
      , round(sums.wins / sums.games, 3) as win_loss_percentage
      , sums.times_wild_card_winner
      , sums.times_division_winner
      , sums.times_league_champion
      , sums.times_world_series_champion
      , sums.runs_scored
      , sums.hits
      , sums.doubles
      , sums.triples
      , sums.home_runs
      , sums.walks
      , sums.strikeouts
      , sums.stolen_bases
      , sums.caught_stealing
      , case
            when sums.stolen_bases + sums.caught_stealing = 0 then 0
            else sums.stolen_bases / (sums.stolen_bases + sums.caught_stealing)
        end as stolen_base_percentage
      , sums.hit_by_pitch
      , sums.sacrifice_flies
      , sums.runs_allowed
      , round(sums.runs_allowed / sums.outs_pitched * 27, 2) as run_average
      , sums.earned_runs_allowed
      , round(sums.earned_runs_allowed / sums.outs_pitched * 27, 2) as earned_run_average
      , sums.complete_games
      , sums.shutouts
      , sums.saves
      , sums.outs_pitched
      , sums.hits_allowed
      , sums.home_runs_allowed
      , sums.walks_allowed
      , sums.strikeouts_pitched
      , sums.errors
      , sums.double_plays

    from sums

)

select * from transformed
