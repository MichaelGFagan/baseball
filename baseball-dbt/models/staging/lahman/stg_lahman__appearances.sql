with source as (

    select * from {{ source('lahman', 'appearances') }}

),

renamed as (

    select
        yearid as year_id,
        teamid as team_id,
        lgid as league_id,
        playerid as person_id,
        g_all as games,
        gs as games_started,
        g_batted as games_batted,
        g_defense as games_on_defense,
        g_p as games_as_pitcher,
        g_c as games_as_catcher,
        g_1b as games_as_first_base,
        g_2b as games_as_second_base,
        g_3b as games_as_third_base,
        g_ss as games_as_shortstop,
        g_lf as games_as_left_field,
        g_cf as games_as_center_field,
        g_rf as games_as_right_field,
        g_of as games_as_outfielder,
        g_dh as games_as_designated_hitter,
        g_ph as games_as_pinch_hitter,
        g_pr as games_as_pinch_runner

    from source

)

select * from renamed
