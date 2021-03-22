{{
    config(
        materialized="table"
    )
}}

{% set start_year = 1871 %}
{% set end_year = 2020 %}


with source as (
            
    {% for year in range(start_year, end_year) %}
    
        {% set table_name = 'retrosheet_' ~ year %}
        
        select *, 'regular_season' as game_type from {{ source('retrosheet', table_name) }}
            
        union all

    {% endfor %}
    
    select *, 'all_star_game' as game_type from {{ source('retrosheet', 'retrosheet_all_star_games') }}
    
    union all 
    
    select *, 'division_series' as game_type from {{ source('retrosheet', 'retrosheet_division_series_games') }}
    
    union all 
    
    select *, 'league_championship_series' as game_type from {{ source('retrosheet', 'retrosheet_league_championship_games') }}
    
    union all 
    
    select *, 'wild_card_series' as game_type from {{ source('retrosheet', 'retrosheet_wild_card_games') }}
    
    union all 
    
    select *, 'world_series' as game_type from {{ source('retrosheet', 'retrosheet_world_series_games') }}
    
),

chadwick as (

    select * from {{ ref('util_chadwick__register') }}

),

transformed as (

    select
        md5(concat(source.date, source.visiting_team, source.home_team, source.game_num)) as retrosheet_game_id
      , date(cast(substring(source.date, 1, 4) as int64), cast(substring(source.date, 5, 2) as int64), cast(substring(source.date, 7, 2) as int64)) as date
      , cast(source.game_num as int64) as date_game_index
      , source.day_of_week
      , source.game_type
      , source.visiting_team
      , source.visiting_team_league
      , cast(source.visiting_team_game_num as int64) as visiting_team_game_number
      , source.home_team
      , source.home_team_league
      , cast(source.home_team_game_num as int64) as home_team_game_number
      , cast(source.visiting_score as int64) as visiting_score
      , cast(source.home_score as int64) as home_score
      , cast(source.num_outs as int64) as number_of_outs
      , cast(if(source.day_night = 'D', 1, 0) as bool) as is_day_game
      , source.completion_info
      , source.forfeit_info
      , source.protest_info
      , source.park_id as retrosheet_ballpark_id
      , cast(source.attendance as int64) as attendance
      , cast(source.time_of_game_minutes as int64) as game_length_minutes
      , source.visiting_line_score as visiting_score_line
      , source.home_line_score as home_score_line
      , cast(source.visiting_abs as int64) as visiting_at_bats
      , cast(source.visiting_hits as int64) as visiting_hits
      , cast(source.visiting_doubles as int64) as visiting_doubles
      , cast(source.visiting_triples as int64) as visiting_triples
      , cast(source.visiting_homeruns as int64) as visiting_home_runs
      , cast(source.visiting_rbi as int64) as visiting_runs_batted_in
      , cast(source.visiting_sac_hits as int64) as visiting_sacrifice_hits
      , cast(source.visiting_sac_flies as int64) as visiting_sacrifice_flies
      , cast(source.visiting_hbp as int64) as visiting_hit_by_pitches
      , cast(source.visiting_bb as int64) as visiting_walks
      , cast(source.visiting_iw as int64) as visiting_intentional_walks
      , cast(source.visiting_k as int64) as visiting_strikeouts
      , cast(source.visiting_sb as int64) as visiting_stolen_bases
      , cast(source.visiting_cs as int64) as visiting_caught_stealing
      , cast(source.visiting_gdp as int64) as visiting_ground_into_double_plays
      , cast(source.visiting_ci as int64) as visiting_reached_on_catcher_interference
      , cast(source.visiting_lob as int64) as visiting_left_on_base
      , cast(source.visiting_pitchers_used as int64) as visiting_pitchers_used
      , cast(source.visiting_individual_er as int64) as visiting_individual_earned_runs
      , cast(source.visiting_er as int64) as visiting_earned_runs
      , cast(source.visiting_wp as int64) as visiting_wild_pitches
      , cast(source.visiting_balks as int64) as visiting_balks
      , cast(source.visiting_po as int64) as visiting_putouts
      , cast(source.visiting_assists as int64) as visiting_assists
      , cast(source.visiting_errors as int64) as visiting_errors
      , cast(source.visiting_pb as int64) as visiting_passed_balls
      , cast(source.visiting_dp as int64) as visiting_double_plays
      , cast(source.visiting_tp as int64) as visiting_triple_plays
      , cast(source.home_abs as int64) as home_at_bats
      , cast(source.home_hits as int64) as home_hits
      , cast(source.home_doubles as int64) as home_doubles
      , cast(source.home_triples as int64) as home_triples
      , cast(source.home_homeruns as int64) as home_home_runs
      , cast(source.home_rbi as int64) as home_runs_batted_in
      , cast(source.home_sac_hits as int64) as home_sacrifice_hits
      , cast(source.home_sac_flies as int64) as home_sacrifice_flies
      , cast(source.home_hbp as int64) as home_hit_by_pitches
      , cast(source.home_bb as int64) as home_walks
      , cast(source.home_iw as int64) as home_intentional_walks
      , cast(source.home_k as int64) as home_strikeouts
      , cast(source.home_sb as int64) as home_stolen_bases
      , cast(source.home_cs as int64) as home_caught_stealing
      , cast(source.home_gdp as int64) as home_ground_into_double_plays
      , cast(source.home_ci as int64) as home_reached_on_catcher_interference
      , cast(source.home_lob as int64) as home_left_on_base
      , cast(source.home_pitchers_used as int64) as home_pitchers_used
      , cast(source.home_individual_er as int64) as home_individual_earned_runs
      , cast(source.home_er as int64) as home_earned_runs
      , cast(source.home_wp as int64) as home_wild_pitches
      , cast(source.home_balks as int64) as home_balks
      , cast(source.home_po as int64) as home_putouts
      , cast(source.home_assists as int64) as home_assists
      , cast(source.home_errors as int64) as home_errors
      , cast(source.home_pb as int64) as home_passed_balls
      , cast(source.home_dp as int64) as home_double_plays
      , cast(source.home_tp as int64) as home_triple_plays
      , source.ump_home_id as home_plate_umpire_retrosheet_id
      , source.ump_home_name as home_plate_umpire
      , source.ump_first_id as first_base_umpire_retrosheet_id
      , source.ump_first_name as first_base_umpire
      , source.ump_second_id as second_base_umpire_retrosheet_id
      , source.ump_second_name as second_base_umpire
      , source.ump_third_id as third_base_umpire_retrosheet_id
      , source.ump_third_name as third_base_umpire
      , source.ump_lf_id as left_field_umpire_retrosheet_id
      , nullif(source.ump_lf_name, '(none)') as left_field_umpire
      , source.ump_rf_id as right_field_umpire_retrosheet_id
      , nullif(source.ump_rf_name, '(none)') as right_field_umpire
      , source.visiting_manager_id as visiting_manager_retrosheet_id
      , source.visiting_manager_name as visiting_manager
      , source.home_manager_id as home_manager_retrosheet_id
      , source.home_manager_name as home_manager
      , source.winning_pitcher_id as winning_pitcher_retrosheet_id
      , source.winning_pitcher_name as winning_pitcher
      , source.losing_pitcher_id as losing_pitcher_retrosheet_id
      , source.losing_pitcher_name as losing_pitcher
      , source.save_pitcher_id as save_pitcher_retrosheet_id
      , nullif(source.save_pitcher_name, '(none)') as save_pitcher
      , source.game_winning_rbi_id as game_winning_run_batted_in_retrosheet_id
      , nullif(source.game_winning_rbi_name, '(none)') as game_winning_run_batted_in
      , source.visiting_starting_pitcher_id as visiting_starting_pitcher_retrosheet_id
      , source.visiting_starting_pitcher_name as visiting_starting_pitcher
      , source.home_starting_pitcher_id as home_starting_pitcher_retrosheet_id
      , source.home_starting_pitcher_name as home_starting_pitcher
      , source.visiting_1_id as visiting_batter_1_retrosheet_id
      , source.visiting_1_name as visiting_batter_1
      , source.visiting_1_pos as visiting_batter_1_position
      , source.visiting_2_id as visiting_batter_2_retrosheet_id
      , source.visiting_2_name as visiting_batter_2
      , source.visiting_2_pos as visiting_batter_2_position
      , source.visiting_3_id as visiting_batter_3_retrosheet_id
      , source.visiting_3_name as visiting_batter_3
      , source.visiting_3_pos as visiting_batter_3_position
      , source.visiting_4_id as visiting_batter_4_retrosheet_id
      , source.visiting_4_name as visiting_batter_4
      , source.visiting_4_pos as visiting_batter_4_position
      , source.visiting_5_id as visiting_batter_5_retrosheet_id
      , source.visiting_5_name as visiting_batter_5
      , source.visiting_5_pos as visiting_batter_5_position
      , source.visiting_6_id as visiting_batter_6_retrosheet_id
      , source.visiting_6_name as visiting_batter_6
      , source.visiting_6_pos as visiting_batter_6_position
      , source.visiting_7_id as visiting_batter_7_retrosheet_id
      , source.visiting_7_name as visiting_batter_7
      , source.visiting_7_pos as visiting_batter_7_position
      , source.visiting_8_id as visiting_batter_8_retrosheet_id
      , source.visiting_8_name as visiting_batter_8
      , source.visiting_8_pos as visiting_batter_8_position
      , source.visiting_9_id as visiting_batter_9_retrosheet_id
      , source.visiting_9_name as visiting_batter_9
      , source.visiting_9_pos as visiting_batter_9_position
      , source.home_1_id as home_batter_1_retrosheet_id
      , source.home_1_name as home_batter_1
      , source.home_1_pos as home_batter_1_position
      , source.home_2_id as home_batter_2_retrosheet_id
      , source.home_2_name as home_batter_2
      , source.home_2_pos as home_batter_2_position
      , source.home_3_id as home_batter_3_retrosheet_id
      , source.home_3_name as home_batter_3
      , source.home_3_pos as home_batter_3_position
      , source.home_4_id as home_batter_4_retrosheet_id
      , source.home_4_name as home_batter_4
      , source.home_4_pos as home_batter_4_position
      , source.home_5_id as home_batter_5_retrosheet_id
      , source.home_5_name as home_batter_5
      , source.home_5_pos as home_batter_5_position
      , source.home_6_id as home_batter_6_retrosheet_id
      , source.home_6_name as home_batter_6
      , source.home_6_pos as home_batter_6_position
      , source.home_7_id as home_batter_7_retrosheet_id
      , source.home_7_name as home_batter_7
      , source.home_7_pos as home_batter_7_position
      , source.home_8_id as home_batter_8_retrosheet_id
      , source.home_8_name as home_batter_8
      , source.home_8_pos as home_batter_8_position
      , source.home_9_id as home_batter_9_retrosheet_id
      , source.home_9_name as home_batter_9
      , source.home_9_pos as home_batter_9_position
      , source.misc as game_notes
      , source.acquisition_info
      
    from source

)

select * from transformed