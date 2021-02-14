with source as (

    select * from {{ source('lahman', 'people') }}

),

chadwick as (

    select * from {{ ref('util_chadwick__register') }}

),

transformed as (

    select
        source.playerid as lahman_id
      , source.retroid as retrosheet_id
      , source.bbrefid as baseball_reference_id
      , source.namefirst as first_name
      , source.namelast as last_name
      , source.namegiven as given_name
      , cast(source.weight as int64) as weight
      , cast(source.height as int64) as height
      , source.bats
      , source.throws
      , source.debut as debut_game_date
      , source.finalgame as final_game_date
      , date(cast(source.birthyear as int64), cast(source.birthmonth as int64), cast(source.birthday as int64)) as date_of_birth
      , cast(source.birthyear as int64) as birth_year
      , cast(source.birthmonth as int64) as birth_month
      , cast(source.birthday as int64) as birth_day
      , source.birthcountry as birth_country
      , source.birthstate as birth_state
      , source.birthcity as birth_city
      , date(cast(source.deathyear as int64), cast(source.deathmonth as int64), cast(source.deathday as int64)) as date_of_death
      , cast(source.deathyear as int64) as death_year
      , cast(source.deathmonth as int64) as death_month
      , cast(source.deathday as int64) as death_day
      , source.deathcountry as death_country
      , source.deathstate as death_state
      , source.deathcity as death_city

    from source as s

),

final as (

    select
        chadwick.person_id
      , transformed.*

    from transformed
    left join chadwick
        transformed.lahman_id = chadwick.lahman_id

)

select * from final
