{{
    config(
        materialized="table"
    )
}}

{% set start_year = 1871 %}
{% set end_year = var('current_year') %}

with source as (

    {% for year in range(start_year, end_year + 1) %}

        {% set table_name = 'batting_' ~ year %}

        {% if loop.last %}

            select * from {{ source('fangraphs', table_name) }} as source

        {% else %}

            select * from {{ source('fangraphs', table_name) }} as source

            union all

        {% endif %}

    {% endfor %}

),

chadwick as (

        select * from {{ ref('util_chadwick__register') }}

),

transformed as (

    select
        chadwick.person_id
      , cast(source.idfg as int64) as fangraphs_id
      , cast(source.season as int64) as year_id
      , source.name
      , source.team
      , cast(source.age as int64) as age
      , source.age_rng as age_range
      , cast(source.g as int64) as games
      , cast(source.ab as int64) as at_bats
      , cast(source.pa as int64) as plate_appearances
      , cast(source.h as int64) as hits
      , cast(source._1b as int64) as singles
      , cast(source._2b as int64) as doubles
      , cast(source._3b as int64) as triples
      , cast(source.hr as int64) as home_runs
      , cast(source.r as int64) as runs
      , cast(source.rbi as int64) as runs_batted_in
      , cast(source.bb as int64) as walks
      , cast(source.ibb as int64) as intentional_walks
      , cast(source.so as int64) as strikeouts
      , cast(source.hbp as int64) as hit_by_pitches
      , cast(source.sf as int64) as sacrifice_flies
      , cast(source.sh as int64) as sacrifice_hits
      , cast(source.gdp as int64) as ground_into_double_plays
      , cast(source.sb as int64) as stolen_bases
      , cast(source.cs as int64) as caught_stealing
      , round(cast(source.avg as numeric), 3) as batting_average
      , cast(source.gb as int64) as ground_balls
      , cast(source.fb as int64) as fly_balls
      , cast(source.ld as int64) as line_drives
      , cast(source.iffb as int64) as infield_fly_balls
      , cast(source.pitches as int64) as pitches
      , cast(source.balls as int64) as balls
      , cast(source.strikes as int64) as strikes
      , cast(source.ifh as int64) as infield_hits
      , cast(source.bu as int64) as bunts
      , cast(source.buh as int64) as bunt_hits
      , round(cast(source.bb_pct as numeric), 3) as walk_percentage
      , round(cast(source.k_pct as numeric), 3) as strikeout_percentage
      , round(cast(source.bb_k as numeric), 3) as walks_per_strikeout
      , round(cast(source.obp as numeric), 3) as on_base_percentage
      , round(cast(source.slg as numeric), 3) as slugging_percentage
      , round(cast(source.ops as numeric), 3) as on_base_plus_slugging
      , round(cast(source.iso as numeric), 3) as isolated_power
      , round(cast(source.babip as numeric), 3) as batting_average_on_balls_in_play
      , round(cast(source.gb_fb as numeric), 3) as ground_balls_per_fly_ball
      , round(cast(source.ld_pct as numeric), 3) as line_drive_percentage
      , round(cast(source.gb_pct as numeric), 3) as ground_ball_percentage
      , round(cast(source.fb_pct as numeric), 3) as fly_ball_percentage
      , round(cast(source.iffb_pct as numeric), 3) as infield_fly_ball_percentage
      , round(cast(source.hr_fb as numeric), 3) as home_runs_per_fly_ball
      , round(cast(source.ifh as numeric), 3) as infield_hit_percentage
      , round(cast(source.buh_pct as numeric), 3) as bunt_hit_percentage
      , round(cast(source.tto_pct as numeric), 3) as three_true_outcomes_percentage
      , round(cast(source.pull_pct as numeric), 3) as pull_percentage
      , round(cast(source.cent_pct as numeric), 3) as straightaway_percentage
      , round(cast(source.oppo_pct as numeric), 3) as opposite_field_percentage
      , round(cast(source.soft_pct as numeric), 3) as soft_hit_percentage
      , round(cast(source.med_pct as numeric), 3) as medium_hit_percentage
      , round(cast(source.hard_pct as numeric), 3) as hard_hit_percentage
      , round(cast(source.ubr as numeric), 1) as ultimate_base_running_runs_above_average
      , round(cast(source.wgdp as numeric), 1) as ground_into_double_plays_runs_above_average
      , round(cast(source.wsb as numeric), 1) as stolen_bases_runs_above_average
      , cast(source.wrc as int64) as woba_runs_created
      , round(cast(source.wraa as numeric), 1) as woba_runs_above_average
      , round(cast(source.woba as numeric), 3) as weighted_on_base_average
      , cast(source.wrc_plus as int64) as woba_runs_created_plus
      , round(cast(source.bat as numeric), 1) as batting_runs_above_average
      , round(cast(source.bsr as numeric), 1) as base_running_runs_above_average
      , round(cast(source.fld as numeric), 1) as fielding_runs_above_average
      , round(cast(source.pos as numeric), 1) as positional_runs_adjustment
      , round(cast(source.off as numeric), 1) as offense_runs_above_average
      , round(cast(source.def as numeric), 1) as defense_runs_above_average
      , round(cast(source.rar as numeric), 1) as runs_above_replacement
      , round(cast(source.lg as numeric), 1) as league_zero_out_adjustment
      , round(cast(source.rep as numeric), 1) as replacement_runs
      , round(cast(source.war as numeric), 1) as wins_above_replacement
      , round(cast(regexp_replace(source.dol, '[\\$\\(\\)]', '') as numeric), 1) as wins_above_replacement_dollar_value
      , round(cast(source.spd as numeric), 1) as speed_score
      , round(cast(source.wpa as numeric), 2) as win_probability_added
      , round(cast(source.minus_wpa as numeric), 2) as wpa_loss_advancement
      , round(cast(source.plus_wpa as numeric), 2) as wpa_win_advancement
      , round(cast(source.re24 as numeric), 2) as run_expectancy_24
      , round(cast(source.rew as numeric), 2) as run_expectancy_wins
      , round(cast(source.pli as numeric), 2) as average_leverage_index
      , round(cast(source.phli as numeric), 2) as average_leverage_index_pinch_hitting
      , cast(source.ph as int64) as pinch_hit_opportunities
      , round(cast(source.wpa_li as numeric), 2) as context_neutral_wins
      , round(cast(source.clutch as numeric), 2) as clutch
      , round(cast(source.fb_pct_pitch as numeric), 2) as fastball_pct_bis
      , round(cast(source.sl_pct as numeric), 2) as slider_percentage_bis
      , round(cast(source.ct_pct as numeric), 2) as cutter_percentage_bis
      , round(cast(source.cb_pct as numeric), 2) as curveball_percentage_bis
      , round(cast(source.ch_pct as numeric), 2) as changeup_percentage_bis
      , round(cast(source.sf_pct as numeric), 2) as split_finger_percentage_bis
      , round(cast(source.kn_pct as numeric), 2) as knuckleball_percentage_bis
      , round(cast(source.xx_pct as numeric), 2) as unknown_percentage_bis
      , round(cast(source.fbv as numeric), 2) as fastball_average_velocity_mph_bis
      , round(cast(source.slv as numeric), 2) as slider_average_velocity_mph_bis
      , round(cast(source.ctv as numeric), 2) as cutter_average_velocity_mph_bis
      , round(cast(source.cbv as numeric), 2) as curveball_average_velocity_mph_bis
      , round(cast(source.chv as numeric), 2) as changeup_average_velocity_mph_bis
      , round(cast(source.sfv as numeric), 2) as split_finger_average_velocity_mph_bis
      , round(cast(source.knv as numeric), 2) as knuckleball_average_velocity_mph_bis
      , round(cast(source.wfb as numeric), 1) as fastball_runs_above_average_mph_bis
      , round(cast(source.wsl as numeric), 1) as slider_runs_above_average_mph_bis
      , round(cast(source.wct as numeric), 1) as cutter_runs_above_average_mph_bis
      , round(cast(source.wcb as numeric), 1) as curveball_runs_above_average_mph_bis
      , round(cast(source.wch as numeric), 1) as changeup_runs_above_average_mph_bis
      , round(cast(source.wsf as numeric), 1) as split_finger_runs_above_average_mph_bis
      , round(cast(source.wkn as numeric), 1) as knuckleball_runs_above_average_mph_bis
      , round(cast(source.wfb_c as numeric), 1) as fastball_runs_above_average_per_100_pitches_bis
      , round(cast(source.wsl_c as numeric), 1) as slider_runs_above_average_per_100_pitches_bis
      , round(cast(source.wct_c as numeric), 1) as cutter_runs_above_average_per_100_pitches_bis
      , round(cast(source.wcb_c as numeric), 1) as curveball_runs_above_average_per_100_pitches_bis
      , round(cast(source.wch_c as numeric), 1) as changeup_runs_above_average_per_100_pitches_bis
      , round(cast(source.wsf_c as numeric), 1) as split_finger_runs_above_average_per_100_pitches_bis
      , round(cast(source.wkn_c as numeric), 1) as knuckleball_runs_above_average_per_100_pitches_bis
      , round(cast(source.swing_pct as numeric), 3) as swing_percentage_bis
      , round(cast(source.o_swing_pct as numeric), 3) as outside_zone_swing_percentage_bis
      , round(cast(source.z_swing_pct as numeric), 3) as inside_zone_swing_percentage_bis
      , round(cast(source.contact_pct as numeric), 3) as contact_percentage_bis
      , round(cast(source.o_contact_pct as numeric), 3) as outside_zone_contact_percentage_bis
      , round(cast(source.z_swing_pct as numeric), 3) as inside_zone_contact_percentage_bis
      , round(cast(source.zone_pct as numeric), 3) as pitches_seen_inside_zone_percentage_bis
      , round(cast(source.f_strike_pct as numeric), 3) as first_pitch_strike_percentage_bis
      , round(cast(source.swstr_pct as numeric), 3) as swinging_strike_percentage_bis
      -- called strike %
      -- called + swinging strike %
      , round(cast(source.fa_pct_sc as numeric), 3) as four_seamer_percentage_sc
      , round(cast(source.ft_pct_sc as numeric), 3) as two_seamer_percentage_sc
      , round(cast(source.fc_pct_sc as numeric), 3) as cutter_percentage_sc
      , round(cast(source.fs_pct_sc as numeric), 3) as splitter_fastball_percentage_sc
      , round(cast(source.fo_pct_sc as numeric), 3) as forkball_percentage_sc
      , round(cast(source.si_pct_sc as numeric), 3) as sinker_percentage_sc
      , round(cast(source.sl_pct_sc as numeric), 3) as slider_percentage_sc
      , round(cast(source.cu_pct_sc as numeric), 3) as curveball_percentage_sc
      , round(cast(source.kc_pct_sc as numeric), 3) as knuckle_curve_percentage_sc
      , round(cast(source.ep_pct_sc as numeric), 3) as eephus_percentage_sc
      , round(cast(source.ch_pct_sc as numeric), 3) as changeup_percentage_sc
      , round(cast(source.sc_pct_sc as numeric), 3) as screwball_percentage_sc
      , round(cast(source.kn_pct_sc as numeric), 3) as knuckleball_percentage_sc
      , round(cast(source.un_pct_sc as numeric), 3) as unknown_pitch_percentage_sc
      , round(cast(source.vfa_sc as numeric), 1) as four_seamer_average_velocity_mph_sc
      , round(cast(source.vft_sc as numeric), 1) as two_seamer_average_velocity_mph_sc
      , round(cast(source.vfc_sc as numeric), 1) as cutter_average_velocity_mph_sc
      , round(cast(source.vfs_sc as numeric), 1) as splitter_fastball_average_velocity_mph_sc
      , round(cast(source.vfo_sc as numeric), 1) as forkball_average_velocity_mph_sc
      , round(cast(source.vsi_sc as numeric), 1) as sinker_average_velocity_mph_sc
      , round(cast(source.vsl_sc as numeric), 1) as slider_average_velocity_mph_sc
      , round(cast(source.vcu_sc as numeric), 1) as curveball_average_velocity_mph_sc
      , round(cast(source.vkc_sc as numeric), 1) as knuckle_curve_average_velocity_mph_sc
      , round(cast(source.vep_sc as numeric), 1) as eephus_average_velocity_mph_sc
      , round(cast(source.vch_sc as numeric), 1) as changeup_average_velocity_mph_sc
      , round(cast(source.vsc_sc as numeric), 1) as screwball_average_velocity_mph_sc
      , round(cast(source.vkn_sc as numeric), 1) as knuckleball_average_velocity_mph_sc
      , round(cast(source.fa_x_sc as numeric), 1) as four_seamer_average_horiztonal_movement_inches_sc
      , round(cast(source.ft_x_sc as numeric), 1) as two_seamer_average_horiztonal_movement_inches_sc
      , round(cast(source.fc_x_sc as numeric), 1) as cutter_average_horiztonal_movement_inches_sc
      , round(cast(source.fs_x_sc as numeric), 1) as splitter_average_horiztonal_movement_inches_sc
      , round(cast(source.fo_x_sc as numeric), 1) as forkball_average_horiztonal_movement_inches_sc
      , round(cast(source.si_x_sc as numeric), 1) as sinker_average_horiztonal_movement_inches_sc
      , round(cast(source.sl_x_sc as numeric), 1) as slider_average_horiztonal_movement_inches_sc
      , round(cast(source.cu_x_sc as numeric), 1) as curveball_average_horiztonal_movement_inches_sc
      , round(cast(source.kc_x_sc as numeric), 1) as knuckle_curve_average_horiztonal_movement_inches_sc
      , round(cast(source.ep_x_sc as numeric), 1) as eephus_average_horiztonal_movement_inches_sc
      , round(cast(source.ch_x_sc as numeric), 1) as changeup_average_horiztonal_movement_inches_sc
      , round(cast(source.sc_x_sc as numeric), 1) as screwball_average_horiztonal_movement_inches_sc
      , round(cast(source.kn_x_sc as numeric), 1) as knuckleball_average_horiztonal_movement_inches_sc
      , round(cast(source.fa_z_sc as numeric), 1) as four_seamer_average_vertical_movement_inches_sc
      , round(cast(source.ft_z_sc as numeric), 1) as two_seamer_average_vertical_movement_inches_sc
      , round(cast(source.fc_z_sc as numeric), 1) as cutter_average_vertical_movement_inches_sc
      , round(cast(source.fs_z_sc as numeric), 1) as splitter_average_vertical_movement_inches_sc
      , round(cast(source.fo_z_sc as numeric), 1) as forkball_average_vertical_movement_inches_sc
      , round(cast(source.si_z_sc as numeric), 1) as sinker_average_vertical_movement_inches_sc
      , round(cast(source.sl_z_sc as numeric), 1) as slider_average_vertical_movement_inches_sc
      , round(cast(source.cu_z_sc as numeric), 1) as curveball_average_vertical_movement_inches_sc
      , round(cast(source.kc_z_sc as numeric), 1) as knuckle_curve_average_vertical_movement_inches_sc
      , round(cast(source.ep_z_sc as numeric), 1) as eephus_average_vertical_movement_inches_sc
      , round(cast(source.ch_z_sc as numeric), 1) as changeup_average_vertical_movement_inches_sc
      , round(cast(source.sc_z_sc as numeric), 1) as screwball_average_vertical_movement_inches_sc
      , round(cast(source.kn_z_sc as numeric), 1) as knuckleball_average_vertical_movement_inches_sc
      , round(cast(source.wfa_sc as numeric), 1) as four_seamer_runs_above_average_sc
      , round(cast(source.wft_sc as numeric), 1) as two_seamer_runs_above_average_sc
      , round(cast(source.wfc_sc as numeric), 1) as cutter_runs_above_average_sc
      , round(cast(source.wfs_sc as numeric), 1) as splitter_runs_above_average_sc
      , round(cast(source.wfo_sc as numeric), 1) as forkball_runs_above_average_sc
      , round(cast(source.wsi_sc as numeric), 1) as sinker_runs_above_average_sc
      , round(cast(source.wsl_sc as numeric), 1) as slider_runs_above_average_sc
      , round(cast(source.wcu_sc as numeric), 1) as curveball_runs_above_average_sc
      , round(cast(source.wkc_sc as numeric), 1) as knuckle_curve_runs_above_average_sc
      , round(cast(source.wep_sc as numeric), 1) as eephus_runs_above_average_sc
      , round(cast(source.wch_sc as numeric), 1) as changeup_runs_above_average_sc
      , round(cast(source.wsc_sc as numeric), 1) as screwball_runs_above_average_sc
      , round(cast(source.wkn_sc as numeric), 1) as knuckleball_runs_above_average_sc
      , round(cast(source.wfa_c_sc as numeric), 1) as four_seamer_runs_above_average_per_100_pitches_sc
      , round(cast(source.wft_c_sc as numeric), 1) as two_seamer_runs_above_average_per_100_pitches_sc
      , round(cast(source.wfc_c_sc as numeric), 1) as cutter_runs_above_average_per_100_pitches_sc
      , round(cast(source.wfs_c_sc as numeric), 1) as splitter_runs_above_average_per_100_pitches_sc
      , round(cast(source.wfo_c_sc as numeric), 1) as forkball_runs_above_average_per_100_pitches_sc
      , round(cast(source.wsi_c_sc as numeric), 1) as sinker_runs_above_average_per_100_pitches_sc
      , round(cast(source.wsl_c_sc as numeric), 1) as slider_runs_above_average_per_100_pitches_sc
      , round(cast(source.wcu_c_sc as numeric), 1) as curveball_runs_above_average_per_100_pitches_sc
      , round(cast(source.wkc_c_sc as numeric), 1) as knuckle_curve_runs_above_average_per_100_pitches_sc
      , round(cast(source.wep_c_sc as numeric), 1) as eephus_runs_above_average_per_100_pitches_sc
      , round(cast(source.wch_c_sc as numeric), 1) as changeup_runs_above_average_per_100_pitches_sc
      , round(cast(source.wsc_c_sc as numeric), 1) as screwball_runs_above_average_per_100_pitches_sc
      , round(cast(source.wkn_c_sc as numeric), 1) as knuckleball_runs_above_average_per_100_pitches_sc
      , round(cast(source.swing_pct_sc as numeric), 3) as swing_percentage_sc
      , round(cast(source.o_swing_pct_sc as numeric), 3) as outside_zone_swing_percentage_sc
      , round(cast(source.z_swing_pct_sc as numeric), 3) as inside_zone_swing_percentage_sc
      , round(cast(source.contact_pct_sc as numeric), 3) as contact_percentage_sc
      , round(cast(source.o_contact_pct_sc as numeric), 3) as outside_zone_contact_percentage_sc
      , round(cast(source.z_contact_pct_sc as numeric), 3) as inside_zone_contact_percentage_sc
      , round(cast(source.zone_pct_sc as numeric), 3) as pitches_seen_inside_zone_percentage_sc
      , round(cast(source.pace as numeric), 1) as average_time_between_pitches_seconds_sc
      , round(cast(source.fa_pct_pi as numeric), 3) as four_seamer_percentage_pi
      , round(cast(source.fc_pct_pi as numeric), 3) as cutter_percentage_pi
      , round(cast(source.fs_pct_pi as numeric), 3) as splitter_percentage_pi
      , round(cast(source.si_pct_pi as numeric), 3) as sinker_percentage_pi
      , round(cast(source.ch_pct_pi as numeric), 3) as changeup_percentage_pi
      , round(cast(source.sl_pct_pi as numeric), 3) as slider_percentage_pi
      , round(cast(source.cu_pct_pi as numeric), 3) as curveball_percentage_pi
      , round(cast(source.cs_pct_pi as numeric), 3) as slow_curveball_percentage_pi
      , round(cast(source.kn_pct_pi as numeric), 3) as knuckleball_percentage_pi
      , round(cast(source.sb_pct_pi as numeric), 3) as screwball_percentage_pi
      , round(cast(source.xx_pct_pi as numeric), 3) as unknown_percentage_pi
      , round(cast(source.vfa_pi as numeric), 3) as four_seamer_average_velocity_mph_pi
      , round(cast(source.vfc_pi as numeric), 3) as cutter_average_velocity_mph_pi
      , round(cast(source.vfs_pi as numeric), 3) as splitter_average_velocity_mph_pi
      , round(cast(source.vsi_pi as numeric), 3) as sinker_average_velocity_mph_pi
      , round(cast(source.vch_pi as numeric), 3) as changeup_average_velocity_mph_pi
      , round(cast(source.vsl_pi as numeric), 3) as slider_average_velocity_mph_pi
      , round(cast(source.vcu_pi as numeric), 3) as curveball_average_velocity_mph_pi
      , round(cast(source.vcs_pi as numeric), 3) as slow_curveball_average_velocity_mph_pi
      , round(cast(source.vkn_pi as numeric), 3) as knuckleball_average_velocity_mph_pi
      , round(cast(source.vsb_pi as numeric), 3) as screwball_average_velocity_mph_pi
      , round(cast(source.vxx_pi as numeric), 3) as unknown_average_velocity_mph_pi
      , round(cast(source.fa_x_pi as numeric), 3) as four_seamer_average_horizontal_movement_inches_pi
      , round(cast(source.fc_x_pi as numeric), 3) as cutter_average_horizontal_movement_inches_pi
      , round(cast(source.fs_x_pi as numeric), 3) as splitter_average_horizontal_movement_inches_pi
      , round(cast(source.si_x_pi as numeric), 3) as sinker_average_horizontal_movement_inches_pi
      , round(cast(source.ch_x_pi as numeric), 3) as changeup_average_horizontal_movement_inches_pi
      , round(cast(source.sl_x_pi as numeric), 3) as slider_average_horizontal_movement_inches_pi
      , round(cast(source.cu_x_pi as numeric), 3) as curveball_average_horizontal_movement_inches_pi
      , round(cast(source.cs_x_pi as numeric), 3) as slow_curveball_average_horizontal_movement_inches_pi
      , round(cast(source.kn_x_pi as numeric), 3) as knuckleball_average_horizontal_movement_inches_pi
      , round(cast(source.sb_x_pi as numeric), 3) as screwball_average_horizontal_movement_inches_pi
      , round(cast(source.xx_x_pi as numeric), 3) as unknown_average_horizontal_movement_inches_pi
      , round(cast(source.fa_z_pi as numeric), 3) as four_seamer_average_vertical_movement_inches_pi
      , round(cast(source.fc_z_pi as numeric), 3) as cutter_average_vertical_movement_inches_pi
      , round(cast(source.fs_z_pi as numeric), 3) as splitter_average_vertical_movement_inches_pi
      , round(cast(source.si_z_pi as numeric), 3) as sinker_average_vertical_movement_inches_pi
      , round(cast(source.ch_z_pi as numeric), 3) as changeup_average_vertical_movement_inches_pi
      , round(cast(source.sl_z_pi as numeric), 3) as slider_average_vertical_movement_inches_pi
      , round(cast(source.cu_z_pi as numeric), 3) as curveball_average_vertical_movement_inches_pi
      , round(cast(source.cs_z_pi as numeric), 3) as slow_curveball_average_vertical_movement_inches_pi
      , round(cast(source.kn_z_pi as numeric), 3) as knuckleball_average_vertical_movement_inches_pi
      , round(cast(source.sb_z_pi as numeric), 3) as screwball_average_vertical_movement_inches_pi
      , round(cast(source.xx_z_pi as numeric), 3) as unknown_average_vertical_movement_inches_pi
      , round(cast(source.wfa_pi as numeric), 3) as four_seamer_runs_above_average_pi
      , round(cast(source.wfc_pi as numeric), 3) as cutter_runs_above_average_pi
      , round(cast(source.wfs_pi as numeric), 3) as splitter_runs_above_average_pi
      , round(cast(source.wsi_pi as numeric), 3) as sinker_runs_above_average_pi
      , round(cast(source.wch_pi as numeric), 3) as changeup_runs_above_average_pi
      , round(cast(source.wsl_pi as numeric), 3) as slider_runs_above_average_pi
      , round(cast(source.wcu_pi as numeric), 3) as curveball_runs_above_average_pi
      , round(cast(source.wcs_pi as numeric), 3) as slow_curveball_runs_above_average_pi
      , round(cast(source.wkn_pi as numeric), 3) as knuckleball_runs_above_average_pi
      , round(cast(source.wsb_pi as numeric), 3) as screwball_runs_above_average_pi
      , round(cast(source.wxx_pi as numeric), 3) as unknown_runs_above_average_pi
      , round(cast(source.wfa_c_pi as numeric), 3) as four_seamer_runs_above_average_per_100_pitches_pi
      , round(cast(source.wfc_c_pi as numeric), 3) as cutter_runs_above_average_per_100_pitches_pi
      , round(cast(source.wfs_c_pi as numeric), 3) as splitter_runs_above_average_per_100_pitches_pi
      , round(cast(source.wsi_c_pi as numeric), 3) as sinker_runs_above_average_per_100_pitches_pi
      , round(cast(source.wch_c_pi as numeric), 3) as changeup_runs_above_average_per_100_pitches_pi
      , round(cast(source.wsl_c_pi as numeric), 3) as slider_runs_above_average_per_100_pitches_pi
      , round(cast(source.wcu_c_pi as numeric), 3) as curveball_runs_above_average_per_100_pitches_pi
      , round(cast(source.wcs_c_pi as numeric), 3) as slow_curveball_runs_above_average_per_100_pitches_pi
      , round(cast(source.wkn_c_pi as numeric), 3) as knuckleball_runs_above_average_per_100_pitches_pi
      , round(cast(source.wsb_c_pi as numeric), 3) as screwball_runs_above_average_per_100_pitches_pi
      , round(cast(source.wxx_c_pi as numeric), 3) as unknown_runs_above_average_per_100_pitches_pi
      , round(cast(source.swing_pct_pi as numeric), 3) as swing_percentage_pi
      , round(cast(source.o_swing_pct_pi as numeric), 3) as outside_zone_swing_percentage_pi
      , round(cast(source.z_swing_pct_pi as numeric), 3) as inside_zone_swing_percentage_pi
      , round(cast(source.contact_pct_pi as numeric), 3) as contact_percentage_pi
      , round(cast(source.o_contact_pct_pi as numeric), 3) as outside_zone_contact_percentage_pi
      , round(cast(source.z_swing_pct_pi as numeric), 3) as inside_zone_contact_percentage_pi
      , round(cast(source.pace_pi as numeric), 1) as average_time_between_pitches_seconds_pi
      , round(cast(source.frm as numeric), 1) as framing_runs
      , cast(source.avg_plus as int64) as batting_average_plus
      , cast(source.bb_pct_plus as int64) as walk_percentage_plus
      , cast(source.k_pct_plus as int64) as strikeout_percentage_plus
      , cast(source.obp_plus as int64) as on_base_percentage_plus
      , cast(source.slg_plus as int64) as slugging_percentage_plus
      , cast(source.iso_plus as int64) as isolated_slugging_plus
      , cast(source.babip_plus as int64) as batting_average_on_balls_in_play_plus
      , cast(round(cast(source.ld_pct_plus as numeric), 2) as int64) as line_drive_percentage_plus
      , cast(source.gb_pct_plus as int64) as groundball_percentage_plus
      , cast(source.fb_pct_plus as int64) as flyball_percentage_plus
      , cast(source.hr_fb_pct_plus as int64) as home_runs_per_fly_ball_plus
      , cast(source.pull_pct_plus as int64) as pull_percentage_plus
      , cast(source.cent_pct_plus as int64) as straightaway_percentage_plus
      , cast(source.oppo_pct_plus as int64) as opposite_field_percentage_plus
      , cast(source.soft_pct_plus as int64) as soft_hit_percentage_plus
      , cast(source.med_pct_plus as int64) as medium_hit_percentage_plus
      , cast(source.hard_pct_plus as int64) as hard_hit_percentage_plus
      , round(cast(source.ev as numeric), 1) as average_exit_velocity
      , round(cast(source.maxev as numeric), 1) as max_exit_velocity
      , round(cast(source.la as numeric), 1) as average_launch_angle
      , cast(source.barrels as int64) as barrels
      , round(cast(source.barrel_pct as numeric), 3) as barrel_percentage
      , cast(source.hardhit as int64) as hard_hit_balls
      , round(cast(source.hardhit_pct as numeric), 3) as hard_hit_ball_percentage
      , cast(source.events as int64) as statcast_batted_balls

    from source
    left join chadwick
        on cast(source.IDfg as int64) = chadwick.fangraphs_id

),

final as (

    select
        {{ dbt_utils.surrogate_key(['person_id', 'year_id']) }} as player_year_id
      , *

    from transformed

)

select * from final
