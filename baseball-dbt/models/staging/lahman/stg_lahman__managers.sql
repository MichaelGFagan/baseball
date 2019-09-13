with source as (

    select * from {{ source('lahman', 'managers') }}

),

renamed as (

    select
        playerid as person_id,
        yearid as year_id,
        teamid as team_id,
        lgid as league_id,
        inseason as order_in_season,
        g as gams,
        w as wins,
        l as losses,
        rank as position_in_standings,
        plyrmgr as is_player_manager

    from source

)

select * from renamed
