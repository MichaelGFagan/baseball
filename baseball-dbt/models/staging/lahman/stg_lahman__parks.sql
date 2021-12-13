with source as (

    select * from {{ source('lahman', 'parks') }}

),

renamed as (

    select
        park_key as park_id
      , park_name as name
      , park_alias as alias
      , city
      , state
      , country

    from source

)

select * from renamed