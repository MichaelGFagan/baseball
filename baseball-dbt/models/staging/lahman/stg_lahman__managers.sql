with source as (

    select * from {{ source('lahman', 'managers') }}

),

renamed as (

    select
        s.playerid as baseball_reference_id,
        s.teamid as team_id,
        s.lgid as league_id,
        cast(s.yearid as int64) as year_id,
        s.inseason as order_in_season,
        s.g as games,
        s.w as wins,
        s.l as losses,
        s.rank as position_in_standings,
        s.plyrmgr as is_player_manager

    from source as s

)

select * from renamed
