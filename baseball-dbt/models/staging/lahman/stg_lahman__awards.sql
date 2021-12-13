with source_players as (

    select * from {{ source('lahman', 'awards_players') }}

),

source_managers as (

    select * from {{ source('lahman', 'awards_managers') }}

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
        unioned.playerid as lahman_id
      , unioned.yearid as year_id
      , unioned.lgid as league_id
      , unioned.awardid as award_name
      , unioned.notes
      , case
            when unioned.tie = 'Y' then true
            when unioned.tie = 'N' then false
        end as is_tie
      , is_player_award


    from unioned

),

final as (

    select
        {{ dbt_utils.surrogate_key([
            'renamed.year_id',
            'renamed.league_id',
            'renamed.award_name',
            'renamed.lahman_id']) }} as award_id
      , chadwick.person_id
      , renamed.*

    from renamed
    left join chadwick
        on renamed.lahman_id = chadwick.lahman_id

)

select * from final
