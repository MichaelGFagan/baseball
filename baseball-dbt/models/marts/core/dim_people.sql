with people as (

    select * from {{ ref('stg_lahman__people') }}

),

register as (

    select * from {{ ref('stg_chadwick_register') }}

)

select
    r.person_id,
    r.mlbam_id,
    r.retrosheet_id,
    r.baseball_reference_id,
    r.baseball_reference_minor_league_id,
    r.fangraphs_id,
    r.npb_id,
    r.pro_football_reference_id,
    r.basketball_reference_id,
    r.hockey_reference_id,
    r.findagrave_id,
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

from register as r
left join people as p
    on r.baseball_reference_id = p.baseball_reference_id
