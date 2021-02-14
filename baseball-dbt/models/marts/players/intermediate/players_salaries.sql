with salaries as (

    select * from {{ ref('stg_lahman__salaries') }}

),

transformed as (

    select
        salaries.person_id
      , sum(salaries.salary) as salary

    from salaries

    group by 1

)

select * from transformed
