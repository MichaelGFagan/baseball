{% macro statcast_template() %}

chadwick as (

    select * from {{ ref('util_chadwick__register') }}

),

transformed as (

    select
        md5(concat(cast(cast(source.game_pk as int64) as string), cast(source.index as string))) as statcast_pitch_id
      , cast(source.index as int64) as index
      , case
            when cast(source.sv_id as string) = '_' then null
            else parse_datetime('%y%m%d_%H%M%S', cast(source.sv_id as string))
        end as pitched_at
      , cast(source.game_pk as int64) as game_id
      , cast(source.game_date as date) as game_date
      , cast(source.game_year as int64) as game_year
        -- E = EX, S = ST, R = RS, F = WC, D = DS, L = LC, W = WS
      , cast(source.game_type as string) as game_type
        cast(source.home_team as string) as home_team
      , cast(source.away_team as string) as away_team
      , cast(source.home_score as int64) as home_score
      , cast(source.away_score as int64) as away_score
      , cast(source.post_home_score as int64) as post_home_score
      , cast(source.post_away_score as int64) as post_away_score
      , cast(source.bat_score as int64) as batting_team_score
      , cast(source.fld_score as int64) as fielding_team_score
      , cast(source.stand as string) as batter_stand
      , cast(source.p_throws as string) as pitcher_hand
      , cast(source.inning as int64) as inning
      , cast(source.inning_topbot as string) as half_inning
      , cast(source.at_bat_number as int64) as game_plate_appearance
      , cast(source.pitch_number as int64) as pitch_number
      , cast(source.balls as int64) as balls
      , cast(source.strikes as int64) as strikes
      , cast(source.outs_when_up as int64) as outs
      , cast(source.if_fielding_alignment as string) as infield_alignment
      , cast(source.of_fielding_alignment as string) as outfield_alignment
      , batter.person_id as batter_person_id
      , pitcher.person_id as pitcher_person_id
      , catcher.person_id as catcher_person_id
      , first_base.person_id as first_base_person_id
      , second_base.person_id as second_base_person_id
      , third_base.person_id as third_base_person_id
      , shortstop.person_id as shortstop_person_id
      , left_field.person_id as left_field_person_id
      , center_field.person_id as center_field_person_id
      , right_field.person_id as right_field_person_id
      , runner_on_first.person_id as runner_on_first_person_id
      , runner_on_second.person_id as runner_on_second_person_id
      , runner_on_third.person_id as runner_on_third_person_id
      , cast(source.pitch_type as string) as pitch_type
      , cast(source.pitch_name as string) as pitch_name
      , cast(source.sz_top as numeric) as strike_zone_top
      , cast(source.sz_bot as numeric) as strike_zone_bottom
      , cast(source.release_speed as numeric) as release_speed
      , cast(source.release_spin_rate as int64) as release_spin_rate
      , cast(source.release_extension as numeric) as release_extension
      , cast(source.release_pos_x as numeric) as release_position_x
      , cast(source.release_pos_y as numeric) as release_position_y
      , cast(source.release_pos_z as numeric) as release_position_z
      , cast(source.vx0 as numeric) as velocity_50_x
      , cast(source.vy0 as numeric) as velocity_50_y
      , cast(source.vz0 as numeric) as velocity_50_z
      , cast(source.ax as numeric) as acceleration_50_x
      , cast(source.ay as numeric) as acceleration_50_y
      , cast(source.az as numeric) as acceleration_50_z
      , cast(source.pfx_x as numeric) as movement_x
      , cast(source.pfx_z as numeric) as movement_z
      , cast(source.plate_x as numeric) as plate_x
      , cast(source.plate_z as numeric) as plate_z
      , cast(source.zone as numeric) as strike_zone_location
      , cast(source.effective_speed as numeric) as effective_speed
      , cast(source.events as string) as plate_appearance_result
      , cast(source.des as string) as plate_appearance_result_description
      , cast(source.type as string) as pitch_result
      , cast(source.description as string) as pitch_result_description
      , cast(source.bb_type as string) as batted_ball_type
      , cast(source.hit_location as int64) as initial_fielder
      , cast(source.hc_x as numeric) as hit_coordinate_x
      , cast(source.hc_y as numeric) as hit_coordinate_y
      , cast(source.hit_distance_sc as numeric) as hit_distance
      , cast(source.launch_speed as numeric) as exit_velocity
      , cast(source.launch_angle as int64) as launch_angle
      , cast(source.launch_speed_angle as int64) as launch_speed_angle
      , cast(source.estimated_ba_using_speedangle as numeric) as estimated_batting_average
      , cast(source.estimated_woba_using_speedangle as numeric) as estimated_woba
      , cast(source.woba_value as numeric) as woba_value
      , cast(source.woba_denom as numeric) as woba_denominator
      , cast(source.babip_value as int64) as babip_value
      , cast(source.iso_value as int64) as number_of_extra_bases

    from source
    left join chadwick as batter
        on source.batter = batter.mlbam_id
    left join chadwick as pitcher
        on source.pitcher = pitcher.mlbam_id
    left join chadwick as catcher
        on source.fielder_2 = catcher.mlbam_id
    left join chadwick as first_base
        on source.fielder_3 = first_base.mlbam_id
    left join chadwick as second_base
        on source.fielder_4 = second_base.mlbam_id
    left join chadwick as third_base
        on source.fielder_5 = third_base.mlbam_id
    left join chadwick as shortstop
        on source.fielder_6 = shortstop.mlbam_id
    left join chadwick as left_field
        on source.fielder_7 = left_field.mlbam_id
    left join chadwick as center_field
        on source.fielder_8 = center_field.mlbam_id
    left join chadwick as right_field
        on source.fielder_9 = right_field.mlbam_id
    left join chadwick as runner_on_first
        on source.on_1b = runner_on_first.mlbam_id
    left join chadwick as runner_on_second
        on source.on_2b = runner_on_second.mlbam_id
    left join chadwick as runner_on_third
        on source.on_3b = runner_on_third.mlbam_id

)

select * from transformed

{% endmacro %}
