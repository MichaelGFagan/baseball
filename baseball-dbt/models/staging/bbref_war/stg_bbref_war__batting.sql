with batting as (

    select * from {{ source('bbref_war', 'batting') }}

)

, chadwick as (

    select * from {{ ref('util_chadwick__register') }}

)

, transformed as (

    select
        {{ dbt_utils.surrogate_key(['chadwick.person_id', 
                                    'batting.stint_id', 
                                    'batting.team_id', 
                                    'batting.lg_id', 
                                    'batting.year_id', 
                                    'FALSE']) }} as player_stint_year_id
        , {{ dbt_utils.surrogate_key(['chadwick.person_id', 
                                      'batting.team_id', 
                                      'batting.year_id']) }} as player_year_team_id
        , {{ dbt_utils.surrogate_key(['chadwick.person_id', 
                                      'batting.year_id']) }} as player_year_id
        , chadwick.person_id
        , cast(batting.mlb_id as int64) as mlbam_id
        , batting.player_id as baseball_reference_id
        , batting.stint_id as stint
        , batting.team_id
        , batting.lg_id as league_id
        , batting.year_id
        , false as is_postseason
        , batting.age
        , batting.pa as plate_appearances
        , batting.g as games
        , batting.inn as innings
        , batting.runs_bat as runs_batting
        , batting.runs_br as runs_baserunning
        , batting.runs_dp as runs_ground_into_double_play
        , batting.runs_field as runs_fielding
        , batting.runs_infield
        , batting.runs_outfield
        , batting.runs_catcher
        , batting.runs_good_plays
        , batting.runs_defense
        , batting.runs_position
        , batting.runs_position_p as runs_position_pitcher_adjustment
        , batting.runs_replacement
        , batting.runs_above_rep as runs_above_replacement
        , batting.runs_above_avg as runs_above_average
        , batting.runs_above_avg_off as runs_above_average_offense
        , batting.runs_above_avg_def as runs_above_average_defense
        , batting.waa as wins_above_average
        , batting.waa_off as wins_above_average_offense
        , batting.waa_def as wins_above_average_defense
        , batting.war as wins_above_replacement
        , batting.war_off as wins_above_replacement_offense
        , batting.war_def as wins_above_replacement_defense
        , batting.war_rep as replacement_player_wins_above_replacement
        , batting.salary
        , batting.pitcher as is_pitcher
        , batting.teamrpg as average_team_runs_scored_per_game
        , batting.opprpg as average_team_runs_allowed_per_game
        , batting.opprppa_rep as average_team_runs_allowed_per_plate_appearance_against_replacement  -- noqa: L016
        , batting.opprpg_rep as average_team_runs_allowed_per_game_against_replacement  -- noqa: L016
        , batting.pyth_exponent as pythagenpat_exponent
        , batting.pyth_exponent_rep as pythagenpat_exponent_replacement
        , batting.waa_win_perc as wins_above_average_win_percentage_with_average_team  -- noqa: L016
        , batting.waa_win_perc_off as wins_above_average_win_percentage_with_average_team_offense  -- noqa: L016
        , batting.waa_win_perc_def as wins_above_average_win_percentage_with_average_team_defense  -- noqa: L016
        , batting.waa_win_perc_rep as wins_above_average_win_percentage_with_average_team_replacement  -- noqa: L016
        , batting.ops_plus as on_base_plus_slugging_plus
        , batting.tob_lg as leage_average_player_times_on_base
        , batting.tb_lg as league_average_player_total_bases

    from batting
    left join chadwick
        on batting.player_id = chadwick.baseball_reference_id

)

select * from transformed
