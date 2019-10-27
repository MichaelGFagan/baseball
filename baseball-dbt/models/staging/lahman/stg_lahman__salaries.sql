with source as (

    select * from {{ source('lahman', 'salaries') }}

),

renamed as (

    select
        s.playerid as baseball_reference_id,
        s.teamid as team_id,
        s.lgid as league_id,
        s.yearid as year_id,
        s.salary

    from source as s

)

select * from renamed
