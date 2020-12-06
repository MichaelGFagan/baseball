with people as (

    select * from {{ ref('stg_lahman__people') }}

),

register as (

    select * from {{ ref('util_chadwick__register') }}

),

appearances as (

    select * from {{ ref('dim_people_players_appearances') }}

),

batting as (

    select * from {{ ref('dim_people_players_batting') }}

),

pitching as (

    select * from {{ ref('dim_people_players_pitching') }}

),

fielding as (

    select * from {{ ref('dim_people_players_fielding') }}

),

postseason_batting as (

    select * from {{ ref('dim_people_players_batting_postseason') }}

),

postseason_pitching as (

    select * from {{ ref('dim_people_players_pitching_postseason') }}

),

postseason_fielding as (

    select * from {{ ref('dim_people_players_fielding_postseason') }}

),

salaries as (

    select * from {{ ref('dim_people_players_salaries') }}

),

transformed as (

    select
        r.person_id,
        p.baseball_reference_id,
        p.bats,
        p.throws,
        p.debut_game_date,
        p.final_game_date,
        r.first_pro_game_played,
        r.last_pro_game_played,
        r.first_mlb_game_played,
        r.last_mlb_game_played,
        r.first_college_game_played,
        r.last_college_game_played,
        r.first_pro_game_managed,
        r.last_pro_game_managed,
        r.first_mlb_game_managed,
        r.last_mlb_game_managed,
        r.first_college_game_managed,
        r.last_college_game_managed,
        r.first_pro_game_umpired,
        r.last_pro_game_umpired,
        r.first_mlb_game_umpired,
        r.last_mlb_game_umpired

    from register as r
    left join people as p using (person_id)

),

final as (

    select
        *

    from transformed
    inner join appearances using (person_id)
    left join batting using (person_id)
    left join pitching using (person_id)
    left join fielding using (person_id)
    left join postseason_batting using (person_id)
    left join postseason_pitching using (person_id)
    left join postseason_fielding using (person_id)
    left join salaries using (person_id)

)

select * from final
