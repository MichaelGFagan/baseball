with source as (

    select * from {{ source('lahman', 'franchises') }}

),

renamed as (

    select
        s.franchid as franchise_id,
        s.franchname as franchise_name,
        case
            when s.active = 'Y' then TRUE
            else FALSE
        end as is_active,
        s.naassoc as national_association_team_id

    from source as s

)

select * from renamed
