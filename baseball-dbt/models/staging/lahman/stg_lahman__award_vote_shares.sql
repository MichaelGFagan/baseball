with source_players as (

    select * from {{ source('lahman', 'awards_share_players') }}

),

source_managers as (

    select * from {{ source('lahman', 'awards_share_managers') }}

),

chadwick as (

    select * from {{ ref('util_chadwick__register') }}

),

unioned as (

    select *, true as is_player_award from source_players
    union all
    select *, false as is_player_award from source_managers

),

renamed as (

    select
        {{ dbt_utils.surrogate_key([
            'unioned.yearid',
            'unioned.lgid',
            'unioned.awardid']) }} as award_vote_share_id
      , unioned.awardid as award_name
      , unioned.yearid as year_id
      , unioned.lgid as league_id
      , cast(unioned.pointswon as int64) as points
      , cast(unioned.pointsmax as int64) as max_points
      , cast(unioned.votesfirst as int64) as first_place_votes
      , unioned.is_player_award

    from unioned

)

select * from renamed
