with managers as (

    select * from {{ ref('stg_lahman__managers') }}

),

teams as (

    select * from {{ ref('stg_lahman__teams') }}

),

sums as (

    select
        managers.lahman_id
      , count(distinct managers.year_id) as seasons
      , min(managers.year_id) as debut_year
      , max(managers.year_id) as final_year
      , count(distinct teams.franchise_id) count_franchises_managed
      , sum(managers.games) as games
      , sum(managers.wins) as wins
      , sum(managers.losses) as losses

    from managers
    join teams
        on managers.team_id = teams.team_id
        and managers.year_id = teams.year_id

    group by 1

)

select * from sums
