with source as (

    select * from {{ source('lahman', 'fielding_of') }}

),

chadwick as (

    select * from {{ ref('util_chadwick__register') }}

),

renamed as (

    select
        s.playerid as lahman_id,
        s.stint,
        cast(s.yearid as int64) as year_id,
        s.glf as games_at_left_field,
        s.gcf as games_at_center_field,
        s.grf as games_at_right_field

    from source as s

),

left_field as (

    select
        r.lahman_id,
        r.stint,
        r.year_id,
        'LF' as position,
        7 as position_number,
        r.games_at_left_field as games

    from renamed as r

    where games_at_left_field is not null and games_at_left_field > 0

),

center_field as (

    select
        r.lahman_id,
        r.stint,
        r.year_id,
        'CF' as position,
        8 as position_number,
        r.games_at_center_field as games

    from renamed as r

    where games_at_center_field is not null and games_at_center_field > 0

),

right_field as (

    select
        r.lahman_id,
        r.stint,
        r.year_id,
        'RF' as position,
        9 as position_number,
        r.games_at_right_field as games

    from renamed as r

    where games_at_right_field is not null and games_at_right_field > 0

),

unioned as (

    select * from left_field

    union all

    select * from center_field

    union all

    select * from right_field

),

transformed as (

    select
        c.person_id,
        u.*

    from unioned as u
    left join chadwick as c using (lahman_id)

)

select * from transformed
