with people as (

    select * from {{ ref('stg_lahman__people') }}

),

register as (

    select * from {{ ref('util_chadwick__register') }}

),

appearances as (

    select * from {{ ref('players_appearances') }}

),

batting as (

    select * from {{ ref('players_batting') }}

),

pitching as (

    select * from {{ ref('players_pitching') }}

),

fielding as (

    select * from {{ ref('players_fielding') }}

),

postseason_batting as (

    select * from {{ ref('players_batting_postseason') }}

),

postseason_pitching as (

    select * from {{ ref('players_pitching_postseason') }}

),

postseason_fielding as (

    select * from {{ ref('players_fielding_postseason') }}

),

salaries as (

    select * from {{ ref('players_salaries') }}

),

transformed as (

    select
        register.person_id
      , people.baseball_reference_id
      , people.bats
      , people.throws
      , people.debut_game_date
      , people.final_game_date
      , register.first_pro_game_played
      , register.last_pro_game_played
      , register.first_mlb_game_played
      , register.last_mlb_game_played
      , register.first_college_game_played
      , register.last_college_game_played
      , register.first_pro_game_managed
      , register.last_pro_game_managed
      , register.first_mlb_game_managed
      , register.last_mlb_game_managed
      , register.first_college_game_managed
      , register.last_college_game_managed
      , register.first_pro_game_umpired
      , register.last_pro_game_umpired
      , register.first_mlb_game_umpired
      , register.last_mlb_game_umpired

    from register
    left join people
        register.person_id = people.person_id

),

final as (

    select
        transformed.*
      , {{ dbt_utils.star(from = ref('players_appearances'), except = ['person_id']) }}
      , {{ dbt_utils.star(from = ref('players_batting'), except = ['person_id']) }}
      , {{ dbt_utils.star(from = ref('players_pitching'), except = ['person_id']) }}
      , {{ dbt_utils.star(from = ref('players_fielding'), except = ['person_id']) }}
      , {{ dbt_utils.star(from = ref('players_batting_postseason'), except = ['person_id']) }}
      , {{ dbt_utils.star(from = ref('players_pitching_postseason'), except = ['person_id']) }}
      , {{ dbt_utils.star(from = ref('players_fielding_postseason'), except = ['person_id']) }}
      , {{ dbt_utils.star(from = ref('players_salaries'), except = ['person_id']) }}

    from transformed
    inner join appearances
        on transformed.person_id = appearances.person_id
    left join batting
        on transformed.person_id = batting.person_id
    left join pitching
        on transformed.person_id = pitching.person_id
    left join fielding
        on transformed.person_id = fielding.person_id
    left join postseason_batting
        on transformed.person_id = postseason_batting.person_id
    left join postseason_pitching
        on transformed.person_id = postseason_pitching.person_id
    left join postseason_fielding
        on transformed.person_id = postseason_fielding.person_id
    left join salaries
        on transformed.person_id = salaries.person_id

)

select * from final
