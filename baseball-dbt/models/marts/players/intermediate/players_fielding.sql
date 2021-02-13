with fielding as (

    select * from {{ ref('stg_lahman__fielding') }} where is_postseason = FALSE

),

teams as (

    select * from {{ ref('stg_lahman__teams') }}

),

transformed as (

    select
        f.person_id,
        count(distinct f.position) as positions,
        array_agg(distinct f.position_number ignore nulls order by f.position_number) as position_list,
        {# sum(ifnull(f.games, 0)) as games_as_fielder, #}
        sum(ifnull(f.games_started, 0)) as games_started_as_fielder,
        sum(ifnull(f.outs_at_position, 0)) as outs_at_position,
        concat(cast(trunc(sum(ifnull(f.outs_at_position, 0)) / 3) as string), '.', cast(mod(sum(ifnull(f.outs_at_position, 0)), 3) as string)) as innings,
        sum(ifnull(f.putouts, 0)) as putouts,
        sum(ifnull(f.assists, 0)) as assists,
        sum(ifnull(f.errors, 0)) as errors,
        sum(ifnull(f.double_plays, 0)) as double_plays,
        sum(ifnull(f.triple_plays, 0)) as triple_plays,
        sum(ifnull(f.passed_balls, 0)) as passed_balls,
        {# sum(ifnull(f.wild_pitches, 0)) as wild_pitches, #}
        sum(ifnull(f.stolen_bases_allowed, 0)) as stolen_bases_allowed,
        sum(ifnull(f.caught_stealing, 0)) as runners_caught_stealing

    from fielding as f
    inner join teams as t using (year_id, team_id)

    group by 1

)

select * from transformed
