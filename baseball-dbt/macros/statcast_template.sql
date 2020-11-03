{% macro statcast_template() %}

chadwick as (

    select * from {{ ref('util_chadwick__register') }}

),

transformed as (

    select
        md5(concat(cast(cast(s.game_pk as int64) as string), cast(s.index as string))) as statcast_pitch_id,
        cast(s.index as int64) as index,
        case
            when cast(s.sv_id as string) = '_' then null
            else parse_datetime('%y%m%d_%H%M%S', cast(s.sv_id as string))
        end as pitched_at,
        cast(s.game_pk as int64) as game_id,
        cast(s.game_date as date) as game_date,
        cast(s.game_year as int64) as game_year,
        -- E = EX, S = ST, R = RS, F = WC, D = DS, L = LC, W = WS
        cast(s.game_type as string) as game_type,
        cast(s.home_team as string) as home_team,
        cast(s.away_team as string) as away_team,
        cast(s.home_score as int64) as home_score,
        cast(s.away_score as int64) as away_score,
        cast(s.post_home_score as int64) as post_home_score,
        cast(s.post_away_score as int64) as post_away_score,
        cast(s.bat_score as int64) as batting_team_score,
        cast(s.fld_score as int64) as fielding_team_score,
        cast(s.stand as string) as batter_stand,
        cast(s.p_throws as string) as pitcher_hand,
        cast(s.inning as int64) as inning,
        cast(s.inning_topbot as string) as half_inning,
        cast(s.at_bat_number as int64) as game_plate_appearance,
        cast(s.pitch_number as int64) as pitch_number,
        cast(s.balls as int64) as balls,
        cast(s.strikes as int64) as strikes,
        cast(s.outs_when_up as int64) as outs,
        cast(s.if_fielding_alignment as string) as infield_alignment,
        cast(s.of_fielding_alignment as string) as outfield_alignment,
        c_b.person_id as batter_person_id,
        c_1.person_id as pitcher_person_id,
        c_2.person_id as catcher_person_id,
        c_3.person_id as first_base_person_id,
        c_4.person_id as second_base_person_id,
        c_5.person_id as third_base_person_id,
        c_6.person_id as shortstop_person_id,
        c_7.person_id as left_field_person_id,
        c_8.person_id as center_field_person_id,
        c_9.person_id as right_field_person_id,
        r_1.person_id as runner_on_first_person_id,
        r_2.person_id as runner_on_second_person_id,
        r_3.person_id as runner_on_third_person_id,
        cast(s.pitch_type as string) as pitch_type,
        cast(s.pitch_name as string) as pitch_name,
        cast(s.sz_top as numeric) as strike_zone_top,
        cast(s.sz_bot as numeric) as strike_zone_bottom,
        cast(s.release_speed as numeric) as release_speed,
        cast(s.release_spin_rate as int64) as release_spin_rate,
        cast(s.release_extension as numeric) as release_extension,
        cast(s.release_pos_x as numeric) as release_position_x,
        cast(s.release_pos_y as numeric) as release_position_y,
        cast(s.release_pos_z as numeric) as release_position_z,
        cast(s.vx0 as numeric) as velocity_50_x,
        cast(s.vy0 as numeric) as velocity_50_y,
        cast(s.vz0 as numeric) as velocity_50_z,
        cast(s.ax as numeric) as acceleration_50_x,
        cast(s.ay as numeric) as acceleration_50_y,
        cast(s.az as numeric) as acceleration_50_z,
        cast(s.pfx_x as numeric) as movement_x,
        cast(s.pfx_z as numeric) as movement_z,
        cast(s.plate_x as numeric) as plate_x,
        cast(s.plate_z as numeric) as plate_z,
        cast(s.zone as numeric) as strike_zone_location,
        cast(s.effective_speed as numeric) as effective_speed,
        cast(s.events as string) as plate_appearance_result,
        cast(s.des as string) as plate_appearance_result_description,
        cast(s.type as string) as pitch_result,
        cast(s.description as string) as pitch_result_description,
        cast(s.bb_type as string) as batted_ball_type,
        cast(s.hit_location as int64) as initial_fielder,
        cast(s.hc_x as numeric) as hit_coordinate_x,
        cast(s.hc_y as numeric) as hit_coordinate_y,
        cast(s.hit_distance_sc as numeric) as hit_distance,
        cast(s.launch_speed as numeric) as exit_velocity,
        cast(s.launch_angle as int64) as launch_angle,
        cast(s.launch_speed_angle as int64) as launch_speed_angle,
        cast(s.estimated_ba_using_speedangle as numeric) as estimated_batting_average,
        cast(s.estimated_woba_using_speedangle as numeric) as estimated_woba,
        cast(s.woba_value as numeric) as woba_value,
        cast(s.woba_denom as numeric) as woba_denominator,
        cast(s.babip_value as int64) as babip_value,
        cast(s.iso_value as int64) as number_of_extra_bases

    from source as s
    left join chadwick as c_b
        on s.batter = c_b.mlbam_id
    left join chadwick as c_1
        on s.pitcher = c_1.mlbam_id
    left join chadwick as c_2
        on s.fielder_2 = c_2.mlbam_id
    left join chadwick as c_3
        on s.fielder_3 = c_3.mlbam_id
    left join chadwick as c_4
        on s.fielder_4 = c_4.mlbam_id
    left join chadwick as c_5
        on s.fielder_5 = c_5.mlbam_id
    left join chadwick as c_6
        on s.fielder_6 = c_6.mlbam_id
    left join chadwick as c_7
        on s.fielder_7 = c_7.mlbam_id
    left join chadwick as c_8
        on s.fielder_8 = c_8.mlbam_id
    left join chadwick as c_9
        on s.fielder_9 = c_9.mlbam_id
    left join chadwick as r_1
        on s.on_1b = r_1.mlbam_id
    left join chadwick as r_2
        on s.on_2b = r_2.mlbam_id
    left join chadwick as r_3
        on s.on_3b = r_3.mlbam_id

)

select * from transformed

{% endmacro %}
