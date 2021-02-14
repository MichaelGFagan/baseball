with people as (

    select * from {{ ref('stg_lahman__people') }}

),

register as (

    select * from {{ ref('util_chadwick__register') }}

),

transformed as (

    select
        register.person_id
      , register.lahman_id
      , register.mlbam_id
      , register.retrosheet_id
      , register.baseball_reference_id
      , register.baseball_reference_minor_league_id
      , register.fangraphs_id
      , register.npb_id
      , register.pro_football_reference_id
      , register.basketball_reference_id
      , register.hockey_reference_id
      , register.findagrave_id
      , register.last_name
      , register.first_name
      , register.given_name
      , register.suffix
      , register.matrilineal_name
      , register.birth_date
      , people.birth_city
      , people.birth_state
      , people.birth_country
      , register.death_date
      , people.death_city
      , people.death_state
      , people.death_country
      , people.weight
      , people.height

    from register
    left join people
        on register.baseball_reference_id = people.baseball_reference_id

)

select * from transformed
