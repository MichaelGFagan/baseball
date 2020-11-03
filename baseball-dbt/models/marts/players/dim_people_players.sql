with people as (

    select * from {{ ref('stg_lahman__people') }}

),

register as (

    select * from {{ ref('util_chadwick__register') }}

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

)

select * from transformed
