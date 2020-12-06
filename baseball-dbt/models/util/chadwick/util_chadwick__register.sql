with source as (

    select * from {{ source('chadwick', 'register') }}

),

lahman as (

    select * from {{ source('lahman', 'people') }}

),

transformed as (

    select
        s.key_uuid as person_id,
        l.playerid as lahman_id,
        s.key_mlbam as mlbam_id,
        s.key_retro as retrosheet_id,
        s.key_bbref as baseball_reference_id,
        s.key_bbref_minors as baseball_reference_minor_league_id,
        s.key_fangraphs as fangraphs_id,
        s.key_npb as npb_id,
        s.key_sr_nfl as pro_football_reference_id,
        s.key_sr_nba as basketball_reference_id,
        s.key_sr_nhl as hockey_reference_id,
        s.key_findagrave as findagrave_id,
        s.name_last as last_name,
        s.name_first as first_name,
        s.name_given as given_name,
        s.name_suffix as suffix,
        s.name_matrilineal as matrilineal_name,
        if(s.birth_year is not null and s.birth_month is not null and s.birth_day is not null, date(cast(s.birth_year as int64), cast(s.birth_month as int64), cast(s.birth_day as int64)), null) as birth_date,
        if(s.death_year is not null and s.death_month is not null and s.death_day is not null, date(cast(s.death_year as int64), cast(s.death_month as int64), cast(s.death_day as int64)), null) as death_date,
        cast(s.birth_year as int64) as birth_year,
        cast(s.birth_month as int64) as birth_month,
        cast(s.birth_day as int64) as birth_day,
        cast(s.death_year as int64) as death_year,
        cast(s.death_month as int64) as death_month,
        cast(s.death_day as int64) as death_day,
        cast(s.pro_played_first as int64) as first_pro_game_played,
        cast(s.pro_played_last as int64) as last_pro_game_played,
        cast(s.mlb_played_first as int64) as first_mlb_game_played,
        cast(s.mlb_played_last as int64) as last_mlb_game_played,
        cast(s.col_played_first as int64) as first_college_game_played,
        cast(s.col_played_last as int64) as last_college_game_played,
        cast(s.pro_managed_first as int64) as first_pro_game_managed,
        cast(s.pro_managed_last as int64) as last_pro_game_managed,
        cast(s.mlb_managed_first as int64) as first_mlb_game_managed,
        cast(s.mlb_managed_last as int64) as last_mlb_game_managed,
        cast(s.col_managed_first as int64) as first_college_game_managed,
        cast(s.col_managed_last as int64) as last_college_game_managed,
        cast(s.pro_umpired_first as int64) as first_pro_game_umpired,
        cast(s.pro_umpired_last as int64) as last_pro_game_umpired,
        cast(s.mlb_umpired_first as int64) as first_mlb_game_umpired,
        cast(s.mlb_umpired_last as int64) as last_mlb_game_umpired

    from source as s
    left join lahman as l
        on s.key_bbref = l.bbrefid

)

select * from transformed
