with source as (

    select * from {{ source('lahman', 'pitching') }}

),

renamed as (

    select
        s.playerid as lahman_id,
        cast(s.stint as string) as stint,
        s.teamid as team_id,
        s.lgid as league_id,
        cast(s.yearid as int64) as year_id,
        FALSE as is_postseason,
        cast(s.w as int64) as wins,
        cast(s.l as int64) as losses,
        cast(s.g as int64) as games,
        cast(s.gs as int64) as games_started,
        cast(s.gf as int64) as games_finished,
        cast(s.cg as int64) as complete_games,
        cast(s.sho as int64) as shutouts,
        cast(s.sv as int64) as saves,
        cast(s.ipouts as int64) as outs_pitched,
        cast(s.h as int64) as hits,
        cast(s.r as int64) as runs,
        cast(s.er as int64) as earned_runs,
        cast(s.hr as int64) as home_runs,
        cast(s.bb as int64) as walks,
        cast(s.so as int64) as strikeouts,
        cast(s.ibb as int64) as intentional_walks,
        cast(s.wp as int64) as wild_pitches,
        cast(s.hbp as int64) as hit_by_pitches,
        cast(s.bk as int64) as balks,
        cast(s.bfp as int64) as batters_faced,
        cast(s.sh as int64) as sacrifice_hits,
        cast(s.sf as int64) as sacrifice_flies,
        cast(s.gidp as int64) as ground_into_double_plays

    from source as s

)

select * from renamed
