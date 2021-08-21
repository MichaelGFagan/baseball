{{
    config(
        materialized="table"
    )
}}

{% set start_year = 2008 %}
{% set end_year = var('current_year') %}


with statcast as (

    {% for year in range(start_year, end_year + 1) %}

        {% set table_name = 'statcast_' ~ year %}

        {% if loop.last %}

            {{ statcast_template() }} from {{ source('statcast', table_name) }} as source

        {% else %}

            {{ statcast_template() }} from {{ source('statcast', table_name) }} as source

            union all

        {% endif %}

    {% endfor %}

),

chadwick as (

    select * from {{ ref('util_chadwick__register') }}

),

transformed as (

    select
        statcast.statcast_pitch_id
      , statcast.index
      , statcast.pitched_at
      , statcast.game_id
      , statcast.game_date
      , statcast.game_year
      , statcast.game_type
      , statcast.home_team
      , statcast.away_team
      , statcast.home_score
      , statcast.away_score
      , statcast.post_home_score
      , statcast.post_away_score
      , statcast.batting_team_score
      , statcast.fielding_team_score
      , statcast.batter_stand
      , statcast.pitcher_hand
      , statcast.inning
      , statcast.half_inning
      , statcast.game_plate_appearance
      , statcast.pitch_number
      , statcast.balls
      , statcast.strikes
      , statcast.outs
      , statcast.infield_alignment
      , statcast.outfield_alignment
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
      , statcast.pitch_type
      , statcast.pitch_name
      , statcast.strike_zone_top
      , statcast.strike_zone_bottom
      , statcast.release_speed
      , statcast.release_spin_rate
      , statcast.release_extension
      , statcast.release_position_x
      , statcast.release_position_y
      , statcast.release_position_z
      , statcast.velocity_50_x
      , statcast.velocity_50_y
      , statcast.velocity_50_z
      , statcast.acceleration_50_x
      , statcast.acceleration_50_y
      , statcast.acceleration_50_z
      , statcast.movement_x
      , statcast.movement_z
      , statcast.plate_x
      , statcast.plate_z
      , statcast.strike_zone_location
      , statcast.effective_speed
      , statcast.plate_appearance_result
      , statcast.plate_appearance_result_description
      , statcast.pitch_result
      , statcast.pitch_result_description
      , statcast.batted_ball_type
      , statcast.initial_fielder
      , statcast.hit_coordinate_x
      , statcast.hit_coordinate_y
      , statcast.hit_distance
      , statcast.exit_velocity
      , statcast.launch_angle
      , statcast.launch_speed_angle
      , statcast.estimated_batting_average
      , statcast.estimated_woba
      , statcast.woba_value
      , statcast.woba_denominator
      , statcast.babip_value
      , statcast.number_of_extra_bases

    from statcast
    left join chadwick as batter
        on statcast.batter_mlbam_id = batter.mlbam_id
    left join chadwick as pitcher
        on statcast.pitcher_mlbam_id = pitcher.mlbam_id
    left join chadwick as catcher
        on statcast.catcher_mlbam_id = catcher.mlbam_id
    left join chadwick as first_base
        on statcast.first_base_mlbam_id = first_base.mlbam_id
    left join chadwick as second_base
        on statcast.second_base_mlbam_id = second_base.mlbam_id
    left join chadwick as third_base
        on statcast.third_base_mlbam_id = third_base.mlbam_id
    left join chadwick as shortstop
        on statcast.shortstop_mlbam_id = shortstop.mlbam_id
    left join chadwick as left_field
        on statcast.left_field_mlbam_id = left_field.mlbam_id
    left join chadwick as center_field
        on statcast.center_field_mlbam_id = center_field.mlbam_id
    left join chadwick as right_field
        on statcast.right_field_mlbam_id = right_field.mlbam_id
    left join chadwick as runner_on_first
        on statcast.runner_on_first_mlbam_id = runner_on_first.mlbam_id
    left join chadwick as runner_on_second
        on statcast.runner_on_second_mlbam_id = runner_on_second.mlbam_id
    left join chadwick as runner_on_third
        on statcast.runner_on_third_mlbam_id = runner_on_third.mlbam_id

)

select * from transformed
