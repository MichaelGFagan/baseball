with source as (

    select * from {{ source('lahman', 'home_games') }}

),

renamed as (

    select
        year_key as year_id
      , league_key as league_id
      , team_key as team_id
      , park_key as park_id
      , span_first as first_game_date
      , span_last as last_game_date
      , games as games
      , games - openings as doubleheaders
      , attendance

    from source

),

final as (

    select
        {{ dbt_utils.surrogate_key(['year_id', 'team_id', 'park_id']) }} as team_home_games_id
      , *

    from renamed

)

select * from final