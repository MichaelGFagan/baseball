with source as (

    select * from {{ source('lahman', 'teams') }}

),

renamed as (

    select
        s.yearid as year_id,
        s.lgid as league_id,
        s.teamid as team_id,
        s.franchid as franchise_id,
        s.divid as division_id,
        s.name,
        s.park,
        s.attendance,
        s.rank as standings_rank,
        s.g as games,
        s.ghome as home_games,
        s.w as wins,
        s.l as losses,
        case
            when s.divwin = 'Y' then TRUE
            when s.divwin = 'N' then FALSE
        end as is_division_winner,
        case
            when s.wcwin = 'Y' then TRUE
            when s.wcwin = 'N' then FALSE
        end as is_wild_card_winner,
        s.lgwin as is_league_champion,
        s.wswin as is_world_series_champion,
        s.r as runs_scored,
        s.ab as at_bats,
        s.h as hits,
        s._2b as doubles,
        s._3b as triples,
        s.hr as home_runs,
        cast(s.bb as int64) as walks,
        s.so as strikeouts,
        cast(s.sb as int64) as stolen_bases,
        cast(s.cs as int64) as caught_stealing,
        cast(s.hbp as int64) as hit_by_pitch,
        cast(s.sf as int64) as sacrifice_flies,
        s.ra as runs_allowed,
        s.er as earned_runs_allowed,
        s.era as earned_run_average,
        s.cg as complete_games,
        s.sho as shutouts,
        s.sv as saves,
        s.ipouts as outs_pitched,
        s.ha as hits_allowed,
        s.hra as home_runs_allowed,
        s.bba as walks_allowed,
        s.soa as strikeouts_pitched,
        s.e as errors,
        s.dp as double_plays,
        s.fp as fielding_percentage,
        s.bpf as batting_park_factor,
        s.ppf as pitching_park_factor

    from source as s

)

select * from renamed
