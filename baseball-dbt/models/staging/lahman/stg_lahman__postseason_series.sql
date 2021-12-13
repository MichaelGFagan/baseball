with source as (

    select * from {{ source('lahman', 'series_post') }}

),

renamed as (

    select
        yearid as year_id
      , round
      , teamidwinner as winner_team_id
      , lgidwinner as winner_league_id
      , teamidloser as loser_team_id
      , lgidloser as loser_league_id
      , wins
      , losses
      , ties

    from source

),

final as (

    select
        {{ dbt_utils.surrogate_key(['year_id', 'round', 'winner_team_id', 'loser_team_id']) }} as postseason_series_id
      , *

    from renamed

)

select * from final