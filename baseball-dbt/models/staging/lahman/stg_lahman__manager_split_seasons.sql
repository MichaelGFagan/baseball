with source as (

    select * from {{ source('lahman', 'managers_half') }}

),

chadwick as (

    select * from {{ ref('util_chadwick__register') }}

),

renamed as (

    select
        playerid as lahman_id
      , teamid as team_id
      , lgid as league_id
      , cast(yearid as int64) as year_id
      , inseason as order_in_season
      , half as season_half
      , g as games
      , w as wins
      , l as losses
      , rank as position_in_standings

    from source

),

final as (

    select
        {{ dbt_utils.surrogate_key(['chadwick.person_id', 'renamed.team_id', 'renamed.league_id', 'renamed.year_id', 'renamed.order_in_season', 'renamed.season_half']) }} as manager_split_season_id
      , chadwick.person_id
      , renamed.*

    from renamed
    left join chadwick
        on renamed.lahman_id = chadwick.lahman_id

)

select * from final
