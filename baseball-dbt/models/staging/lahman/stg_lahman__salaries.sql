with source as (

    select * from {{ source('lahman', 'salaries') }}

),

renamed as (

    select
        s.playerid as person_id,
        s.yearid as year_id,
        s.teamid as team_id,
        s.lgid as league_id,
        s.salary

    from source as s

)

select * from renamed
