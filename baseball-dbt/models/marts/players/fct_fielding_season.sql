with fielding as (

    select * from {{ ref('stg_lahman__fielding') }}

),

transformed as (

    select
        fielding.person_id
      , fielding.year_id
      , fielding.is_postseason
      , fielding.position
      , fielding.position_number
      , count(distinct fielding.team_id) as teams
      , sum(fielding.games) as games
      , sum(fielding.games_started) as games_started
      , sum(fielding.outs_at_position) as outs_at_position
      , concat(cast(trunc(sum(fielding.outs_at_position) / 3) as string), '.', cast(mod(sum(fielding.outs_at_position), 3) as string)) as innings
      , sum(fielding.putouts) as putouts
      , sum(fielding.assists) as assists
      , sum(fielding.errors) as errors
      , sum(fielding.double_plays) as double_plays
      , sum(fielding.triple_plays) as triple_plays
      , sum(fielding.passed_balls) as passed_balls
      , sum(fielding.wild_pitches) as wild_pitches
      , sum(fielding.stolen_bases_allowed) as stolen_bases_allowed
      , sum(fielding.caught_stealing) as caught_stealing

    from fielding

    group by 1, 2, 3, 4, 5

)

select * from transformed
