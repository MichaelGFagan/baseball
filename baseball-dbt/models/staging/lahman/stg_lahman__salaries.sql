with source as (

    select * from {{ source('lahman', 'salaries') }}

),

renamed as (

    select
        playerid as person_id,
        yearid as year_id,
        teamid as team_id,
        lgid as league_id,
        salary

    from source

)

select * from renamed
