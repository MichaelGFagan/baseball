with source as (

    select * from {{ source('lahman', 'people') }}

),

renamed as (

    select
        playerid as player_id,
        retroid as retrosheet_id,
        bbrefid as bb_ref_id,
        namefirst as first_name,
        namelast as last_name,
        namegiven as given_name,
        weight,
        height,
        bats,
        throws,
        debut,
        finalgame as final_game,
        date(birthyear, birthmonth, birthday) as date_of_birth,
        birthyear as birth_year,
        birthmonth as birth_month,
        birthday as birth_day,
        birthcountry as birth_country,
        birthstate as birth_state,
        birthcity as birth_city,
        date(deathyear, deathmonth, deathday) as date_of_death,
        deathyear as death_year,
        deathmonth as death_month,
        deathday as death_day,
        deathcountry as death_country,
        deathstate as death_state,
        deathcity as death_city

    from source

)

select * from renamed
