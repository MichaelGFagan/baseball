with fielding as (

    select * from {{ ref('stg_lahman__fielding') }} where is_postseason = TRUE

),

teams as (

    select * from {{ ref('stg_lahman__teams') }}

),

transformed as (

    select
        f.person_id,
        count(distinct f.position) as postseason_positions,
        array_agg(distinct f.position order by case
                                                   when f.position = 'P' then 1
                                                   when f.position = 'C' then 2
                                                   when f.position = '1B' then 3
                                                   when f.position = '2B' then 4
                                                   when f.position = '3B' then 5
                                                   when f.position = 'SS' then 6
                                                   when f.position = 'LF' then 7
                                                   when f.position = 'CF' then 8
                                                   when f.position = 'RF' then 9
                                               end) as postseason_position_list,
        sum(ifnull(f.games, 0)) as postseason_games_as_fielder,
        sum(ifnull(f.games_started, 0)) as postseason_games_started_as_fielder,
        sum(ifnull(f.outs_at_position, 0)) as postseason_outs,
        concat(cast(trunc(sum(ifnull(f.outs_at_position, 0)) / 3) as string), '.', cast(mod(sum(ifnull(f.outs_at_position, 0)), 3) as string)) as postseason_innings,
        sum(ifnull(f.putouts, 0)) as postseason_putouts,
        sum(ifnull(f.assists, 0)) as postseason_assists,
        sum(ifnull(f.errors, 0)) as postseason_errors,
        sum(ifnull(f.double_plays, 0)) as postseason_double_plays,
        sum(ifnull(f.triple_plays, 0)) as postseason_triple_plays,
        sum(ifnull(f.passed_balls, 0)) as postseason_passed_balls,
        sum(ifnull(f.wild_pitches, 0)) as postseason_wild_pitches,
        sum(ifnull(f.stolen_bases_allowed, 0)) as postseason_stolen_bases_allowed,
        sum(ifnull(f.caught_stealing, 0)) as postseason_caught_stealing

    from fielding as f
    join teams as t using (year_id, team_id)

    group by 1

)

select * from transformed
