with fielding as (

    select * from {{ ref('stg_lahman__fielding') }} where is_postseason = TRUE

),

transformed as (

    select
        fielding.person_id
      , count(distinct fielding.position) as postseason_positions
      , array_agg(distinct fielding.position_number ignore nulls order by fielding.position_number) as postseason_position_list
        {# sum(ifnull(fielding.games, 0)) as postseason_games_as_fielder, #}
      , sum(ifnull(fielding.games_started, 0)) as postseason_games_started_as_fielder
      , sum(ifnull(fielding.outs_at_position, 0)) as postseason_outs
      , concat(cast(trunc(sum(ifnull(fielding.outs_at_position, 0)) / 3) as string), '.', cast(mod(sum(ifnull(fielding.outs_at_position, 0)), 3) as string)) as postseason_innings
      , sum(ifnull(fielding.putouts, 0)) as postseason_putouts
      , sum(ifnull(fielding.assists, 0)) as postseason_assists
      , sum(ifnull(fielding.errors, 0)) as postseason_errors
      , sum(ifnull(fielding.double_plays, 0)) as postseason_double_plays
      , sum(ifnull(fielding.triple_plays, 0)) as postseason_triple_plays
      , sum(ifnull(fielding.passed_balls, 0)) as postseason_passed_balls
        {# sum(ifnull(fielding.wild_pitches, 0)) as postseason_wild_pitches, #}
      , sum(ifnull(fielding.stolen_bases_allowed, 0)) as postseason_stolen_bases_allowed
      , sum(ifnull(fielding.caught_stealing, 0)) as postseason_runners_caught_stealing

    from fielding

    group by 1

)

select * from transformed
