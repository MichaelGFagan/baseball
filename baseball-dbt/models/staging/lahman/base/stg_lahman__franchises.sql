with source as (

    select * from {{ source('lahman', 'franchises') }}

),

renamed as (

    select
        franchid as franchise_id,
        franchname as franchise_name,
        case
            when active = 'Y' then TRUE
            else FALSE
        end as is_active,
        naassoc as national_association_team_id

    from source

)

select * from renamed
