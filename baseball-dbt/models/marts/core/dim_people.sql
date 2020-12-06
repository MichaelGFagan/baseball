with people as (

    select * from {{ ref('stg_lahman__people') }}

),

register as (

    select * from {{ ref('util_chadwick__register') }}

),

transformed as (

    select
        r.person_id,
        r.lahman_id,
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
        r.last_name,
        r.first_name,
        r.given_name,
        r.suffix,
        r.matrilineal_name,
        r.birth_date,
        p.birth_city,
        p.birth_state,
        p.birth_country,
        r.death_date,
        p.death_city,
        p.death_state,
        p.death_country,
        p.weight,
        p.height

    from register as r
    left join people as p
        on r.baseball_reference_id = p.baseball_reference_id

)

select * from transformed
