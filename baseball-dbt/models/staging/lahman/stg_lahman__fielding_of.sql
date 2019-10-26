with source as (

    select * from {{ source('lahman', 'fielding_of') }}

),

renamed as (

    select
        s.playerid as person_id,
        s.yearid as year_id,
        s.stint,
        s.glf as games_at_left_field,
        s.gcf as games_at_center_field,
        s.grf as games_at_right_field

    from source as s

),

left_field as (

    select
        r.person_id,
        r.year_id,
        r.stint,
        'LF' as position,
        r.games_at_left_field as games

    from renamed as r

    where games_at_left_field is not null and games_at_left_field > 0

),

center_field as (

    select
        r.person_id,
        r.year_id,
        r.stint,
        'CF' as position,
        r.games_at_center_field as games

    from renamed as r

    where games_at_center_field is not null and games_at_center_field > 0

),

right_field as (

    select
        r.person_id,
        r.year_id,
        r.stint,
        'RF' as position,
        r.games_at_right_field as games

    from renamed as r

    where games_at_right_field is not null and games_at_right_field > 0

)

select * from left_field

union all

select * from center_field

union all

select * from right_field
