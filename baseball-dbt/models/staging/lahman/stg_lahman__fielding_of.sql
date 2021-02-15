with source as (

    select * from {{ source('lahman', 'fielding_of') }}

),

chadwick as (

    select * from {{ ref('util_chadwick__register') }}

),

transformed as (

    select
        source.playerid as lahman_id
      , source.stint
      , cast(source.yearid as int64) as year_id
      , source.glf as games_at_left_field
      , source.gcf as games_at_center_field
      , source.grf as games_at_right_field

    from source

),

left_field as (

    select
        transformed.lahman_id
      , transformed.stint
      , transformed.year_id
      , 'LF' as position
      , 7 as position_number
      , transformed.games_at_left_field as games

    from transformed

    where transformed.games_at_left_field is not null and transformed.games_at_left_field > 0

),

center_field as (

    select
        transformed.lahman_id
      , transformed.stint
      , transformed.year_id
      , 'CF' as position
      , 8 as position_number
      , transformed.games_at_center_field as games

    from transformed

    where transformed.games_at_center_field is not null and transformed.games_at_center_field > 0

),

right_field as (

    select
        transformed.lahman_id
      , transformed.stint
      , transformed.year_id
      , 'RF' as position
      , 9 as position_number
      , transformed.games_at_right_field as games

    from transformed

    where transformed.games_at_right_field is not null and transformed.games_at_right_field > 0

),

unioned as (

    select * from left_field

    union all

    select * from center_field

    union all

    select * from right_field

),

final as (

    select
        chadwick.person_id
      , unioned.*

    from unioned
    left join chadwick
        on unioned.lahman_id = chadwick.lahman_id

)

select * from final
