with source as (

    select * from {{ source('chadwick', 'register') }}

),

lahman as (

    select * from {{ source('lahman', 'people') }}

),

transformed as (

    select
        source.key_uuid as person_id
      , lahman.playerid as lahman_id
      , source.key_mlbam as mlbam_id
      , source.key_retro as retrosheet_id
      , source.key_bbref as baseball_reference_id
      , source.key_bbref_minors as baseball_reference_minor_league_id
      , source.key_fangraphs as fangraphs_id
      , source.key_npb as npb_id
      , source.key_sr_nfl as pro_football_reference_id
      , source.key_sr_nba as basketball_reference_id
      , source.key_sr_nhl as hockey_reference_id
      , source.key_findagrave as findagrave_id
      , source.name_last as last_name
      , source.name_first as first_name
      , source.name_given as given_name
      , source.name_suffix as suffix
      , source.name_matrilineal as matrilineal_name
      , case
            when source.birth_year is null or source.birth_month is null or source.birth_day is null then null
            when source.birth_year > 2020 or source.birth_year < 1800 then null
            when source.birth_month > 12 or source.birth_month < 1 then null
            when source.birth_day > 31 or source.birth_day < 1 then null
            else date(cast(source.birth_year as int64), cast(source.birth_month as int64), cast(source.birth_day as int64))
        end as birth_date
      , cast(source.birth_year as int64) as birth_year
      , cast(source.birth_month as int64) as birth_month
      , cast(source.birth_day as int64) as birth_day
      , case
            when source.death_year is null or source.death_month is null or source.death_day is null then null
            when source.death_year > 2020 or source.death_year < 1800 then null
            when source.death_month > 12 or source.death_month < 1 then null
            when source.death_day > 31 or source.death_day < 1 then null
            else date(cast(source.death_year as int64), cast(source.death_month as int64), cast(source.death_day as int64))
        end as death_date
      , cast(source.death_year as int64) as death_year
      , cast(source.death_month as int64) as death_month
      , cast(source.death_day as int64) as death_day
      , cast(source.pro_played_first as int64) as first_pro_game_played
      , cast(source.pro_played_last as int64) as last_pro_game_played
      , cast(source.mlb_played_first as int64) as first_mlb_game_played
      , cast(source.mlb_played_last as int64) as last_mlb_game_played
      , cast(source.col_played_first as int64) as first_college_game_played
      , cast(source.col_played_last as int64) as last_college_game_played
      , cast(source.pro_managed_first as int64) as first_pro_game_managed
      , cast(source.pro_managed_last as int64) as last_pro_game_managed
      , cast(source.mlb_managed_first as int64) as first_mlb_game_managed
      , cast(source.mlb_managed_last as int64) as last_mlb_game_managed
      , cast(source.col_managed_first as int64) as first_college_game_managed
      , cast(source.col_managed_last as int64) as last_college_game_managed
      , cast(source.pro_umpired_first as int64) as first_pro_game_umpired
      , cast(source.pro_umpired_last as int64) as last_pro_game_umpired
      , cast(source.mlb_umpired_first as int64) as first_mlb_game_umpired
      , cast(source.mlb_umpired_last as int64) as last_mlb_game_umpired

    from source
    left join lahman
        on source.key_bbref = lahman.bbrefid

)

select * from transformed
