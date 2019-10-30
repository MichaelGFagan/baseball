with fielding as (

    select * from {{ ref('stg_lahman__fielding') }}

),

teams as (

    select * from {{ ref('stg_lahman__teams') }}

)

select
    f.baseball_reference_id,
    f.is_postseason,
    f.position,
    count(distinct f.year_id) as seasons,
    count(distinct t.franchise_id) as franchises,
    sum(ifnull(f.games, 0)) as games,
    sum(ifnull(f.games_started, 0)) as games_started,
    sum(ifnull(f.outs_at_position, 0)) as outs_at_position,
    sum(ifnull(f.putouts, 0)) as putouts,
    sum(ifnull(f.assists, 0)) as assists,
    sum(ifnull(f.errors, 0)) as errors,
    sum(ifnull(f.double_plays, 0)) as double_plays,
    sum(ifnull(f.triple_plays, 0)) as triple_plays,
    sum(ifnull(f.passed_balls, 0)) as passed_balls,
    sum(ifnull(f.wild_pitches, 0)) as wild_pitches,
    sum(ifnull(f.stolen_bases_allowed, 0)) as stolen_bases_allowed,
    sum(ifnull(f.caught_stealing, 0)) as caught_stealing

from fielding as f
join teams as t using (year_id, team_id)

group by 1, 2, 3
