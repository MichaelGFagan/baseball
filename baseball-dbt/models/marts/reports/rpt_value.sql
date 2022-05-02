with bref_batting as (

  select
        player_year_id
      , sum(plate_appearances) as plate_appearances
      , sum(wins_above_replacement) as war_offense_and_defense_bref
      , sum(wins_above_replacement_offense) as war_offense_bref
      , sum(wins_above_replacement_defense) as war_defense_bref

  from {{ ref('stg_bbref_war__batting') }}

  group by 1

)

, bref_pitching as (

    select
        player_year_id
      , sum(outs_pitched) as outs_pitched
      , sum(wins_above_replacement) as war_pitching_bref

    from {{ ref('stg_bbref_war__pitching') }}

    group by 1

),

fg_batting as (

    select
        player_year_id
      , wins_above_replacement as war_offense_and_defense_fg

    from {{ ref('base_fangraphs__batting')}}

),

fg_pitching as (

    select
        player_year_id
      , wins_above_replacement as war_pitching_fg)

    from {{ ref('base_fangraphs__pitching') }}

),

final as (

    select
        coalesce(bref_batting.player_year_id,
                 bref_pitching.player_year_id,
                 fg_batting.player_year_id,
                 fg_pitching.player_year_id) as player_year_id
      , bref_batting.plate_appearances
      , bref_pitching.outs_pitched
      , bref_batting.war_offense_bref
      , bref_batting.war_defense_bref
      , bref_batting.war_offense_and_defense_bref
      , bref_pitching.war_pitching_bref
      , bref_batting.war_offense_and_defense_bref + bref_pitching.war_pitching_bref as war_bref
      , fg_batting.war_offense_and_defense_fg
      , fg_pitching.war_pitching_fg
      , fg_batting.war_offense_and_defense_fg + fg_pitching.war_pitching_fg as war_fg
    
    from bref_batting
    full join bref_pitching
        on bref_batting.player_year_id = bref_pitching.player_year_id
    full join fg_batting
        on bref_batting.player_year_id = fg_batting.player_year_id
    full join fg_pitching
        on bref_batting.player_year_id = fg_pitching.player_year_id

)

select * from final
