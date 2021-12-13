with source as (

    select * from {{ source('lahman', 'all_star_full') }}

),

chadwick as (

    select * from {{ ref('util_chadwick__register') }}

),

renamed as (

    select
        source.playerid as lahman_id
      , cast(source.yearid as int64) as year_id
      , source.lgid as league_id
      , source.teamid as team_id
      , source.gameid as game_id
      , cast(source.gamenum as int64) as game_number
      , source.gp as games_played
      , cast(source.startingpos as int64) as starting_position

    from source

),

final as (

    select
        chadwick.person_id
      , renamed.*

    from renamed
    left join chadwick
        on renamed.lahman_id = chadwick.lahman_id

)

select * from final
