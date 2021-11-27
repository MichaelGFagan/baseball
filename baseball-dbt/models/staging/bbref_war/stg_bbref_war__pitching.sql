with pitching as (

    select * from {{ source('bbref_war', 'pitching') }}

),

chadwick as (

    select * from {{ ref('util_chadwick__register') }}

),

transformed as (

    select
        {{ dbt_utils.surrogate_key(['chadwick.person_id', 'pitching.stint_id', 'pitching.team_id', 'pitching.lg_id', 'pitching.year_id', 'FALSE']) }} as player_stint_year_id
      , {{ dbt_utils.surrogate_key(['chadwick.person_id', 'pitching.team_id', 'pitching.year_id']) }} as player_year_team_id
      , {{ dbt_utils.surrogate_key(['chadwick.person_id', 'pitching.year_id',]) }} as player_year_id
      , chadwick.person_id
      , cast(pitching.mlb_id as int64) as mlbam_id
      , pitching.player_id as baseball_reference_id
      , pitching.stint_id as stint
      , pitching.team_id
      , pitching.lg_id as league_id
      , pitching.year_id
      , FALSE as is_postseason
      , pitching.age
      , pitching.g as games
      , pitching.gs as games_started
      , pitching.ipouts as outs_pitched
      , pitching.ipouts_start as outs_pitched_as_starter
      , pitching.ipouts_relief as outs_pitched_as_reliever
      , pitching.ra as runs_allowed
      , pitching.xra as expected_runs_allowed
      , pitching.xra_sprp_adj as expected_runs_allowed_role_adjustment
      , pitching.xra_extras_adj as expected_runs_allowed_extra_adjustment
      , pitching.xra_def_pitcher as expected_runs_allowed_team_defense_adjustment
      , pitching.ppf as home_park_factor
      , pitching.ppf_custom as pitched_park_factor
      , pitching.xra_final as expected_runs_allowed_final
      , pitching.bip as balls_in_play
      , pitching.bip_perc as percent_of_teams_balls_in_play
      , pitching.rs_def_total as team_defensive_runs_saved
      , pitching.runs_above_avg as runs_above_average
      , pitching.runs_above_avg_adj as runs_above_average_adjusted
      , pitching.runs_above_rep as runs_above_average_multiplied_by_replacement_level_factor
      , pitching.rpo_replacement as runs_per_out_replacement_level
      , pitching.gr_leverage_index_avg as leverage_in_relief_appearances
      , pitching.war as wins_above_replacement
      , pitching.war_rep as replacement_pitcher_wins_above_replacement
      , pitching.salary
      , pitching.teamrpg as average_team_runs_per_game_with_pitcher
      , pitching.opprpg as average_opponent_runs_per_game
      , pitching.opprpg_rep as average_opponent_runs_allowed_per_game_against_replacement
      , pitching.pyth_exponent as pythagenpat_exponent
      , pitching.waa as wins_above_average
      , pitching.waa_adj as wins_above_average_adjusted
      , pitching.waa_win_perc as wins_above_average_win_percentage_with_average_team
      , pitching.era_plus
      , pitching.er_lg as league_average_pitcher_earned_runs

    from pitching
    left join chadwick
        on pitching.player_id = chadwick.baseball_reference_id

)

select * from transformed
