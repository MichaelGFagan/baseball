with source as (

    select * from {{ source('lahman', 'teams') }}

),

renamed as (

    select
        s.teamid as team_id,
        s.franchid as franchise_id,
        s.divid as division_id,
        s.lgid as league_id,
        cast(s.yearid as int64) as year_id,
        s.name,
        s.park,
        cast(s.attendance as int64) as attendance,
        cast(s.rank as int64) as standings_rank,
        cast(s.g as int64) as games,
        cast(s.ghome as int64) as home_games,
        cast(s.w as int64) as wins,
        cast(s.l as int64) as losses,
        case
            when s.divwin = 'Y' then TRUE
            when s.divwin = 'N' then FALSE
        end as is_division_winner,
        case
            when s.wcwin = 'Y' then TRUE
            when s.wcwin = 'N' then FALSE
        end as is_wild_card_winner,
        case
            when s.lgwin = 'Y' then TRUE
            when s.lgwin = 'N' then FALSE
        end as is_league_champion,
        case
            when s.wswin = 'Y' then TRUE
            when s.wswin = 'N' then FALSE
        end as is_world_series_champion,
        cast(s.r as int64) as runs_scored,
        cast(s.ab as int64) as at_bats,
        cast(s.h as int64) as hits,
        cast(s._2b as int64) as doubles,
        cast(s._3b as int64) as triples,
        cast(s.hr as int64) as home_runs,
        cast(s.bb as int64) as walks,
        cast(s.so as int64) as strikeouts,
        cast(s.sb as int64) as stolen_bases,
        cast(s.cs as int64) as caught_stealing,
        cast(s.hbp as int64) as hit_by_pitch,
        cast(s.sf as int64) as sacrifice_flies,
        cast(s.ra as int64) as runs_allowed,
        cast(s.er as int64) as earned_runs_allowed,
        cast(s.era as int64) as earned_run_average,
        cast(s.cg as int64) as complete_games,
        cast(s.sho as int64) as shutouts,
        cast(s.sv as int64) as saves,
        cast(s.ipouts as int64) as outs_pitched,
        cast(s.ha as int64) as hits_allowed,
        cast(s.hra as int64) as home_runs_allowed,
        cast(s.bba as int64) as walks_allowed,
        cast(s.soa as int64) as strikeouts_pitched,
        cast(s.e as int64) as errors,
        cast(s.dp as int64) as double_plays,
        cast(s.fp as int64) as fielding_percentage,
        cast(s.bpf as int64) as batting_park_factor,
        cast(s.ppf as int64) as pitching_park_factor

    from source as s

)

select * from renamed
