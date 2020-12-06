with source as (

    select * from {{ source('lahman', 'people') }}

),

chadwick as (

    select * from {{ ref('util_chadwick__register') }}

),

renamed as (

    select
        s.playerid as lahman_id,
        s.retroid as retrosheet_id,
        s.bbrefid as baseball_reference_id,
        s.namefirst as first_name,
        s.namelast as last_name,
        s.namegiven as given_name,
        cast(s.weight as int64) as weight,
        cast(s.height as int64) as height,
        s.bats,
        s.throws,
        s.debut as debut_game_date,
        s.finalgame as final_game_date,
        date(cast(s.birthyear as int64), cast(s.birthmonth as int64), cast(s.birthday as int64)) as date_of_birth,
        cast(s.birthyear as int64) as birth_year,
        cast(s.birthmonth as int64) as birth_month,
        cast(s.birthday as int64) as birth_day,
        s.birthcountry as birth_country,
        s.birthstate as birth_state,
        s.birthcity as birth_city,
        date(cast(s.deathyear as int64), cast(s.deathmonth as int64), cast(s.deathday as int64)) as date_of_death,
        cast(s.deathyear as int64) as death_year,
        cast(s.deathmonth as int64) as death_month,
        cast(s.deathday as int64) as death_day,
        s.deathcountry as death_country,
        s.deathstate as death_state,
        s.deathcity as death_city

    from source as s

),

transformed as (

    select
        c.person_id,
        r.*

    from renamed as r
    left join chadwick as c using (lahman_id)

)

select * from transformed
