with fielding as (

    select * from {{ ref('stg_lahman__fielding') }}

),

teams as (

    select * from {{ ref('stg_lahman__teams') }}

),

transformed as (

    select
        fielding.person_id,
        fielding.year_id,
        fielding.stint,
        fielding.team_id,
        teams.franchise_id,
        fielding.league_id,
        fielding.is_postseason,
        fielding.position,
        fielding.position_number,
        fielding.games,
        fielding.games_started,
        fielding.outs_at_position,
        concat(cast(trunc(fielding.outs_at_position / 3) as string), '.', cast(mod(fielding.outs_at_position, 3) as string)) as innings,
        fielding.putouts,
        fielding.assists,
        fielding.errors,
        fielding.double_plays,
        fielding.triple_plays,
        fielding.passed_balls,
        fielding.wild_pitches,
        fielding.stolen_bases_allowed,
        fielding.caught_stealing,
        fielding.zone_rating

    from fielding
    inner join teams
        on fielding.year_id = teams.year_id
        and fielding.team_id = teams.team_id

)

select * from transformed
