with source as (

    select * from {{ source('lahman', 'franchises') }}

),

renamed as (

    select
        string_field_0 as franchise_id,
        string_field_1 as franchise_name,
        case
            when string_field_2 = 'Y' then TRUE
            else FALSE
        end as is_active,
        string_field_3 as national_association_team_id

    from source

)

select * from renamed
