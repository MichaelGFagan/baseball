with fielding as (

    select * from {{ ref('stg_lahman__fielding') }}

),

teams as (

    select * from {{ ref('stg_lahman__teams') }}

),

transformed as (

    select
        f.person_id,
        f.year_id,
        f.stint,
        f.team_id,
        t.franchise_id,
        f.league_id,
        f.is_postseason,
        f.position,
        f.position_number,
        f.games,
        f.games_started,
        f.outs_at_position,
        concat(cast(trunc(f.outs_at_position / 3) as string), '.', cast(mod(f.outs_at_position, 3) as string)) as innings,
        f.putouts,
        f.assists,
        f.errors,
        f.double_plays,
        f.triple_plays,
        f.passed_balls,
        f.wild_pitches,
        f.stolen_bases_allowed,
        f.caught_stealing,
        f.zone_rating

    from fielding as f
    inner join teams as t using (year_id, team_id)

)

select * from transformed
