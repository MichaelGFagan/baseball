with source as (

    select * from {{ source('lahman', 'fielding_of') }}

),

renamed as (

    select
        playerid as person_id,
        yearid as year_id,
        stint,
        glf as games_at_left_field,
        gcf as games_at_center_field,
        grf as games_at_right_field

    from source

),

left_field as (

    select
        person_id,
        year_id,
        stint,
        'LF' as position,
        games_at_left_field as games

    from renamed

    where games_at_left_field is not null and games_at_left_field > 0

),

center_field as (

    select
        person_id,
        year_id,
        stint,
        'CF' as position,
        games_at_center_field as games

    from renamed

    where games_at_center_field is not null and games_at_center_field > 0

),

right_field as (

    select
        person_id,
        year_id,
        stint,
        'RF' as position,
        games_at_right_field as games

    from renamed

    where games_at_right_field is not null and games_at_right_field > 0

)

select * from left_field

union all

select * from center_field

union all

select * from right_field
