with people as (

    select * from {{ ref('stg_lahman__people') }}

)

select
    p.person_id,
    p.last_name,
    p.first_name,
    p.given_name,
    p.date_of_birth,
    p.birth_city,
    p.birth_state,
    p.birth_country,
    p.date_of_death,
    p.death_city,
    p.death_state,
    p.death_country,
    p.weight,
    p.height

from people as p
