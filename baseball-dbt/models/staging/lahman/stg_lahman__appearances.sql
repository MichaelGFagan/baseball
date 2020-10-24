with source as (

    select * from {{ source('lahman', 'appearances') }}

),

renamed as (

    select
        s.playerid as baseball_reference_id,
        s.lgid as league_id,
        s.teamid as team_id,
        cast(s.yearid as int64) as year_id,
        s.g_all as games,
        s.gs as games_started,
        s.g_batting as games_as_batter,
        s.g_defense as games_as_defense,
        s.g_p as games_as_pitcher,
        s.g_c as games_as_catcher,
        s.g_1b as games_as_first_base,
        s.g_2b as games_as_second_base,
        s.g_3b as games_as_third_base,
        s.g_ss as games_as_shortstop,
        s.g_lf as games_as_left_field,
        s.g_cf as games_as_center_field,
        s.g_rf as games_as_right_field,
        s.g_of as games_as_outfielder,
        s.g_dh as games_as_designated_hitter,
        s.g_ph as games_as_pinch_hitter,
        s.g_pr as games_as_pinch_runner

    from source as s

)

select * from renamed
