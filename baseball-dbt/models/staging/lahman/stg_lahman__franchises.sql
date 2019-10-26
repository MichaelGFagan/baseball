with source as (

    select * from {{ source('lahman', 'franchises') }}

),

renamed as (

    select
        s.string_field_0 as franchise_id,
        s.string_field_1 as franchise_name,
        case
            when s.string_field_2 = 'Y' then TRUE
            else FALSE
        end as is_active,
        s.string_field_3 as national_association_team_id

    from source as s

)

select * from renamed
