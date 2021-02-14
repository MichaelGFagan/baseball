with source as (

    select * from {{ source('lahman', 'teams') }}

),

transformed as (

    select
        source.teamid as team_id
      , source.franchid as franchise_id
      , source.divid as division_id
      , source.lgid as league_id
      , cast(source.yearid as int64) as year_id
      , source.name
      , source.park
      , cast(source.attendance as int64) as attendance
      , cast(source.rank as int64) as standings_rank
      , cast(source.g as int64) as games
      , cast(source.ghome as int64) as home_games
      , cast(source.w as int64) as wins
      , cast(source.l as int64) as losses
      , case
            when source.divwin = 'Y' then TRUE
            when source.divwin = 'N' then FALSE
        end as is_division_winner
      , case
            when source.wcwin = 'Y' then TRUE
            when source.wcwin = 'N' then FALSE
        end as is_wild_card_winner
      , case
            when source.lgwin = 'Y' then TRUE
            when source.lgwin = 'N' then FALSE
        end as is_league_champion
      , case
            when source.wswin = 'Y' then TRUE
            when source.wswin = 'N' then FALSE
        end as is_world_series_champion
      , cast(source.r as int64) as runs_scored
      , cast(source.ab as int64) as at_bats
      , cast(source.h as int64) as hits
      , cast(source._2b as int64) as doubles
      , cast(source._3b as int64) as triples
      , cast(source.hr as int64) as home_runs
      , cast(source.bb as int64) as walks
      , cast(source.so as int64) as strikeouts
      , cast(source.sb as int64) as stolen_bases
      , cast(source.cs as int64) as caught_stealing
      , cast(source.hbp as int64) as hit_by_pitch
      , cast(source.sf as int64) as sacrifice_flies
      , cast(source.ra as int64) as runs_allowed
      , cast(source.er as int64) as earned_runs_allowed
      , cast(source.era as int64) as earned_run_average
      , cast(source.cg as int64) as complete_games
      , cast(source.sho as int64) as shutouts
      , cast(source.sv as int64) as saves
      , cast(source.ipouts as int64) as outs_pitched
      , cast(source.ha as int64) as hits_allowed
      , cast(source.hra as int64) as home_runs_allowed
      , cast(source.bba as int64) as walks_allowed
      , cast(source.soa as int64) as strikeouts_pitched
      , cast(source.e as int64) as errors
      , cast(source.dp as int64) as double_plays
      , cast(source.fp as int64) as fielding_percentage
      , cast(source.bpf as int64) as batting_park_factor
      , cast(source.ppf as int64) as pitching_park_factor

    from source as s

)

select * from transformed
