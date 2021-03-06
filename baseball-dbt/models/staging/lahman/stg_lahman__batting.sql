with batting as (

    select * from {{ ref('base_lahman__batting') }}

),

batting_post as (

    select * from {{ ref('base_lahman__batting_post') }}

),

chadwick as (

    select * from {{ ref('util_chadwick__register') }}

),

unioned as (

    select * from batting

    union all

    select * from batting_post

),

final as (

    select
        {{ dbt_utils.surrogate_key(['chadwick.person_id', 'unioned.stint', 'unioned.team_id', 'unioned.league_id', 'unioned.year_id', 'FALSE']) }} as player_stint_year_id
      , {{ dbt_utils.surrogate_key(['chadwick.person_id', 'unioned.team_id', 'unioned.year_id', 'FALSE']) }} as player_year_team_id
      , {{ dbt_utils.surrogate_key(['chadwick.person_id', 'unioned.year_id', 'FALSE']) }} as player_year_id
      , chadwick.person_id
      , unioned.*

    from unioned
    left join chadwick
        on unioned.lahman_id = chadwick.lahman_id

)

select * from final
