with salaries as (

    select * from {{ ref('stg_lahman__salaries') }}

),

transformed as (

    select
        salaries.person_id
      , salaries.year_id
      , salaries.team_id
      , salaries.league_id
      , salaries.salary

    from salaries

)

select * from transformed
