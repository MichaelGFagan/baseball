with source as (

    select * from {{ source('lahman', 'people') }}

),

renamed as (

    select
        s.playerid as baseball_reference_id,
        s.namefirst as first_name,
        s.namelast as last_name,
        s.namegiven as given_name,
        s.weight,
        s.height,
        s.bats,
        s.throws,
        s.debut as debut_game_date,
        s.finalgame as final_game_date,
        date(s.birthyear, s.birthmonth, s.birthday) as date_of_birth,
        s.birthyear as birth_year,
        s.birthmonth as birth_month,
        s.birthday as birth_day,
        s.birthcountry as birth_country,
        s.birthstate as birth_state,
        s.birthcity as birth_city,
        date(s.deathyear, s.deathmonth, s.deathday) as date_of_death,
        deathyear as death_year,
        s.deathmonth as death_month,
        s.deathday as death_day,
        s.deathcountry as death_country,
        s.deathstate as death_state,
        s.deathcity as death_city

    from source as s

)

select * from renamed
