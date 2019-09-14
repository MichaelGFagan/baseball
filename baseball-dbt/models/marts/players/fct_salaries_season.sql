with salaries as (

    select * from {{ ref('stg_lahman__salaries') }}

)

select
    s.person_id,
    s.year_id,
    s.team_id,
    s.league_id,
    s.salary

from salaries as s
