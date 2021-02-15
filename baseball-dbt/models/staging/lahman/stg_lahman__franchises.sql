with source as (

    select * from {{ source('lahman', 'franchises') }}

),

transformed as (

    select
        source.franchid as franchise_id
      , source.franchname as franchise_name
      , case
            when source.active = 'Y' then TRUE
            else FALSE
        end as is_active
      , source.naassoc as national_association_team_id

    from source

)

select * from transformed
