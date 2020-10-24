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
        date(cast(s.birthyear as int64), cast(s.birthmonth as int64), cast(s.birthday as int64)) as date_of_birth,
        s.birthyear as birth_year,
        s.birthmonth as birth_month,
        s.birthday as birth_day,
        s.birthcountry as birth_country,
        s.birthstate as birth_state,
        s.birthcity as birth_city,
        date(cast(s.deathyear as int64), cast(s.deathmonth as int64), cast(s.deathday as int64)) as date_of_death,
        s.deathyear as death_year,
        s.deathmonth as death_month,
        s.deathday as death_day,
        s.deathcountry as death_country,
        s.deathstate as death_state,
        s.deathcity as death_city

    from source as s

)

select * from renamed
