with fielding as (

    select * from {{ ref('stg_lahman__fielding') }}

),

teams as (

    select * from {{ ref('stg_lahman__teams') }}

)

select
    f.person_id,
    f.year_id,
    f.is_postseason,
    f.position,
    count(distinct f.team_id) as teams,
    sum(f.games) as games,
    sum(f.games_started) as games_started,
    sum(f.outs_at_position) as outs_at_position,
    sum(f.putouts) as putouts,
    sum(f.assists) as assists,
    sum(f.errors) as errors,
    sum(f.double_plays) as double_plays,
    sum(f.triple_plays) as triple_plays,
    sum(f.passed_balls) as passed_balls,
    sum(f.wild_pitches) as wild_pitches,
    sum(f.stolen_bases_allowed) as stolen_bases_allowed,
    sum(f.caught_stealing) as caught_stealing

from fielding as f
join teams as t using (year_id, team_id)

group by 1, 2, 3, 4
