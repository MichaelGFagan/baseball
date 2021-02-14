with source as (

    select * from {{ source('lahman', 'appearances') }}

),

chadwick as (

    select * from {{ ref('util_chadwick__register') }}

),

renamed as (

    select
        source.playerid as lahman_id
      , source.lgid as league_id
      , source.teamid as team_id
      , cast(source.yearid as int64) as year_id
      , source.g_all as games
      , source.gs as games_started
      , source.g_batting as games_as_batter
      , source.g_defense as games_as_fielder
      , source.g_p as games_as_pitcher
      , source.g_c as games_as_catcher
      , source.g_1b as games_as_first_base
      , source.g_2b as games_as_second_base
      , source.g_3b as games_as_third_base
      , source.g_ss as games_as_shortstop
      , source.g_lf as games_as_left_field
      , source.g_cf as games_as_center_field
      , source.g_rf as games_as_right_field
      , source.g_of as games_as_outfielder
      , source.g_dh as games_as_designated_hitter
      , source.g_ph as games_as_pinch_hitter
      , source.g_pr as games_as_pinch_runner

    from source

),

final as (

    select
        chadwick.person_id
      , renamed.*

    from renamed
    left join chadwick
        on renamed.lahman_id = chadwick.lahman_id

)

select * from final
