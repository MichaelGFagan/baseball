with salaries as (

    select * from {{ ref('stg_lahman__salaries') }}

)

select
    s.person_id,
    sum(s.salary) as salary

from salaries as s

group by 1
