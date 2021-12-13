with source as (

    select * from {{ source('lahman', 'teams_half') }}

),

transformed as (

    select
        teamid as team_id
      , divid as division_id
      , lgid as league_id
      , cast(yearid as int64) as year_id
      , half as season_half
      , rank as position_in_standings
      , g as games
      , w as wins
      , l as losses
      , case
            when divwin = 'Y' then true
            when divwin = 'N' then false
        end as is_division_winner

    from source

)

select * from transformed
