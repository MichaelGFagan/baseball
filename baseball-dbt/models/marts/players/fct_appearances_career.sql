with appearances as (

    select * from {{ ref('stg_lahman__appearances') }}

)

select
    a.person_id,
    count(distinct a.year_id) as seasons,
    count(distinct a.team_id) as teams,
    sum(ifnull(a.games, 0) as games,
    sum(ifnull(a.games_started, 0) as games_started,
    sum(ifnull(a.games_batted, 0) as games_batted,
    sum(ifnull(a.games_on_defense, 0) as games_on_defense,
    sum(ifnull(a.games_as_pitcher, 0) as games_as_pitcher,
    sum(ifnull(a.games_as_catcher, 0) as games_as_catcher,
    sum(ifnull(a.games_as_first_base, 0) as games_as_first_base,
    sum(ifnull(a.games_as_second_base, 0) as games_as_second_base,
    sum(ifnull(a.games_as_third_base, 0) as games_as_third_base,
    sum(ifnull(a.games_as_shortstop, 0) as games_as_shortstop,
    sum(ifnull(a.games_as_left_field, 0) as games_as_left_field,
    sum(ifnull(a.games_as_center_field, 0) as games_as_center_field,
    sum(ifnull(a.games_as_right_field, 0) as games_as_right_field,
    sum(ifnull(a.games_as_outfielder, 0) as games_as_outfielder,
    sum(ifnull(a.games_as_designated_hitter, 0) as games_as_designated_hitter,
    sum(ifnull(a.games_as_pinch_hitter, 0) as games_as_pinch_hitter,
    sum(ifnull(a.games_as_pinch_runner, 0) as games_as_pinch_runner

from appearances as a
