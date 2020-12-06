with source as (

    select * from {{ source('lahman', 'fielding_of_split') }}

),

renamed as (

    select
        s.playerid as lahman_id,
        cast(s.stint as string) as stint,
        s.teamid as team_id,
        s.lgid as league_id,
        cast(s.yearid as int64) as year_id,
        FALSE as is_postseason,
        s.pos as position,
        case
            when s.pos = 'P' then 1
            when s.pos = 'C' then 2
            when s.pos = '1B' then 3
            when s.pos = '2B' then 4
            when s.pos = '3B' then 5
            when s.pos = 'SS' then 6
            when s.pos = 'LF' then 7
            when s.pos = 'CF' then 8
            when s.pos = 'RF' then 9
        end as position_number,
        cast(s.g as int64) as games,
        cast(s.gs as int64) as games_started,
        cast(s.innouts as int64) as outs_at_position,
        cast(s.po as int64) as putouts,
        cast(s.a as int64) as assists,
        cast(s.e as int64) as errors,
        cast(s.dp as int64) as double_plays,
        null as triple_plays,
        cast(s.pb as int64) as passed_balls,
        cast(s.wp as int64) as wild_pitches,
        cast(s.sb as int64) as stolen_bases_allowed,
        cast(s.cs as int64) as caught_stealing,
        cast(s.zr as int64) as zone_rating

    from source as s

)

select * from renamed
