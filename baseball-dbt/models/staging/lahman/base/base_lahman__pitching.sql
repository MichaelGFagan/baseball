with source as (

    select * from {{ source('lahman', 'pitching') }}

),

renamed as (

    select
        s.playerid as baseball_reference_id,
        cast(s.stint as string) as stint,
        s.teamid as team_id,
        s.lgid as league_id,
        s.yearid as year_id,
        FALSE as is_postseason,
        s.w as wins,
        s.l as losses,
        s.g as games,
        s.gs as games_started,
        s.gf as games_finished,
        s.cg as complete_games,
        s.sho as shutouts,
        s.sv as saves,
        s.ipouts as outs_pitched,
        s.h as hits,
        s.r as runs,
        s.er as earned_runs,
        s.hr as home_runs,
        s.bb as walks,
        s.so as strikeouts,
        cast(s.ibb as int64) as intentional_walks,
        s.wp as wild_pitches,
        cast(s.hbp as int64) as hit_by_pitch,
        s.bk as balks,
        s.bfp as batters_faced,
        cast(s.sh as int64) as sacrifice_hits,
        cast(s.sf as int64) as sacrifice_flies,
        cast(s.gidp as int64) as ground_into_double_plays

    from source as s

)

select * from renamed
