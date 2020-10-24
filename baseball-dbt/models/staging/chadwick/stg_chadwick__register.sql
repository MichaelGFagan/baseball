with source as (

    select * from {{ source('chadwick', 'register') }}

),

transformed as (

    select
        s.key_uuid as person_id,
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
        date(cast(s.birth_year as int64), cast(s.birth_month as int64), cast(s.birth_day as int64)) as birth_date,
        date(cast(s.death_year as int64), cast(s.death_month as int64), cast(s.death_day as int64)) as death_date,
        s.pro_played_first as first_pro_game_played,
        s.pro_played_last as last_pro_game_played,
        s.mlb_played_first as first_mlb_game_played,
        s.mlb_played_last as last_mlb_game_played,
        s.col_played_first as first_college_game_played,
        s.col_played_last as last_college_game_played,
        s.pro_managed_first as first_pro_game_managed,
        s.pro_managed_last as last_pro_game_managed,
        s.mlb_managed_first as first_mlb_game_managed,
        s.mlb_managed_last as last_mlb_game_managed,
        s.col_managed_first as first_college_game_managed,
        s.col_managed_last as last_college_game_managed,
        s.pro_umpired_first as first_pro_game_umpired,
        s.pro_umpired_last as last_pro_game_umpired,
        s.mlb_umpired_first as first_mlb_game_umpired,
        s.mlb_umpired_last as last_mlb_game_umpired

    from source as s

)

select * from transformed
