with source as (

    select * from {{ source('lahman', 'teams') }}

),

renamed as (

    select
        yearid as year_id,
        lgid as league_id,
        teamid as team_id,
        franchid as franchise_id,
        divid as division_id,
        name,
        park,
        attendance,
        rank as standings_rank,
        g as games,
        ghome as home_games,
        w as wins,
        l as losses,
        case
            when divwin = 'Y' then TRUE
            when divwin = 'N' then FALSE
        end as is_division_winner,
        case
            when wcwin = 'Y' then TRUE
            when wcwin = 'N' then FALSE
        end as is_wild_card_winner,
        lgwin as is_league_champion,
        wswin as is_world_series_champion,
        r as runs_scored,
        ab as at_bats,
        h as hits,
        _2b as doubles,
        _3b as triples,
        hr as home_runs,
        cast(bb as int64) as walks,
        so as strikeouts,
        cast(sb as int64) as stolen_bases,
        cast(cs as int64) as caught_stealing,
        cast(hbp as int64) as hit_by_pitch,
        cast(sf as int64) as sacrifice_flies,
        ra as runs_allowed,
        er as earned_runs_allowed,
        era as earned_run_average,
        cg as complete_games,
        sho as shutouts,
        sv as saves,
        ipouts as outs_pitched,
        ha as hits_allowed,
        hra as home_runs_allowed,
        bba as walks_allowed,
        soa as strikeouts_pitched,
        e as errors,
        dp as double_plays,
        fp as fielding_percentage,
        bpf as batting_park_factor,
        ppf as pitching_park_factor

    from source

)

select * from renamed
