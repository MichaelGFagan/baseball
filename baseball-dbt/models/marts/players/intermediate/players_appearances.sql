with appearances as (

    select * from {{ ref('stg_lahman__appearances') }}

),

transformed as (

    select
        appearances.person_id
      , count(distinct appearances.year_id) as seasons
      , count(distinct appearances.team_id) as teams
      , array_agg(distinct appearances.team_id ignore nulls order by appearances.team_id) as team_list
      , count(distinct teams.franchise_id) as franchises
      , array_agg(distinct teams.franchise_id order by teams.franchise_id) as franchise_list
      , sum(ifnull(appearances.games, 0)) as games
      , sum(ifnull(appearances.games_started, 0)) as games_started
      , sum(ifnull(appearances.games_as_batter, 0)) as games_as_batter
      , sum(ifnull(appearances.games_as_fielder, 0)) as games_as_fielder
      , sum(ifnull(appearances.games_as_pitcher, 0)) as games_as_pitcher
      , sum(ifnull(appearances.games_as_catcher, 0)) as games_as_catcher
      , sum(ifnull(appearances.games_as_first_base, 0)) as games_as_first_base
      , sum(ifnull(appearances.games_as_second_base, 0)) as games_as_second_base
      , sum(ifnull(appearances.games_as_third_base, 0)) as games_as_third_base
      , sum(ifnull(appearances.games_as_shortstop, 0)) as games_as_shortstop
      , sum(ifnull(appearances.games_as_left_field, 0)) as games_as_left_field
      , sum(ifnull(appearances.games_as_center_field, 0)) as games_as_center_field
      , sum(ifnull(appearances.games_as_right_field, 0)) as games_as_right_field
      , sum(ifnull(appearances.games_as_outfielder, 0)) as games_as_outfielder
      , sum(ifnull(appearances.games_as_designated_hitter, 0)) as games_as_designated_hitter
      , sum(ifnull(appearances.games_as_pinch_hitter, 0)) as games_as_pinch_hitter
      , sum(ifnull(appearances.games_as_pinch_runner, 0)) as games_as_pinch_runner

    from appearances

    group by 1

)

select * from transformed
