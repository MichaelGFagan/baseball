with people as (

    select * from {{ ref('stg_lahman__people') }}

)

select
    p.person_id,
    p.bats,
    p.throws,
    p.debut_game_date,
    p.final_game_date

from people as p