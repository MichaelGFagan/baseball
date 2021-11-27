with bref_batting as (

  select
        person_id
      , year_id
      , {{ same_name_agg('plate_appearances') }}
      , {{ same_name_agg('games') }}
      , {{ same_name_agg('innings') }}
      , {{ same_name_agg('runs_batting') }}
      , {{ same_name_agg('runs_baserunning') }}
      , {{ same_name_agg('runs_ground_into_double_play') }}
      , {{ same_name_agg('runs_fielding') }}
      , {{ same_name_agg('runs_infield') }}
      , {{ same_name_agg('runs_outfield') }}
      , {{ same_name_agg('runs_catcher') }}
      , {{ same_name_agg('runs_good_plays') }}
      , {{ same_name_agg('runs_defense') }}
      , {{ same_name_agg('runs_position') }}
      , {{ same_name_agg('runs_position_pitcher_adjustment') }}
      , {{ same_name_agg('runs_replacement') }}
      , {{ same_name_agg('runs_above_replacement') }}
      , {{ same_name_agg('runs_above_average') }}
      , {{ same_name_agg('runs_above_average_offense') }}
      , {{ same_name_agg('runs_above_average_defense') }}
      , {{ same_name_agg('wins_above_average') }}
      , {{ same_name_agg('wins_above_average_offense') }}
      , {{ same_name_agg('wins_above_average_defense') }}
      , {{ same_name_agg('wins_above_replacement') }}
      , {{ same_name_agg('wins_above_replacement_offense') }}
      , {{ same_name_agg('wins_above_replacement_defense') }}
      , {{ same_name_agg('replacement_player_wins_above_replacement') }}

  from {{ ref('stg_bbref_war__batting') }}

  group by 1, 2

)

, bref_pitching as (

    select
        person_id
      , year_id
      , {{ same_name_agg('games') }}
      , {{ same_name_agg('games_started') }}
      , {{ same_name_agg('outs_pitched') }}
      , {{ same_name_agg('outs_pitched_as_starter') }}
      , {{ same_name_agg('outs_pitched_as_reliever') }}
      , {{ same_name_agg('team_defensive_runs_saved') }}
      , {{ same_name_agg('runs_above_average') }}
      , {{ same_name_agg('runs_above_average_adjusted') }}
      , {{ same_name_agg('runs_above_average_multiplied_by_replacement_level_factor') }}
      , {{ same_name_agg('runs_per_out_replacement_level') }}
      , {{ same_name_agg('leverage_in_relief_appearances') }}
      , {{ same_name_agg('wins_above_replacement') }}
      , {{ same_name_agg('replacement_pitcher_wins_above_replacement') }}

)

select * from bref_batting
