with source as (

    select * from {{ source('lahman', 'schools') }}

),

renamed as (

    select
        schoolid as school_id
      , name_full as name
      , city
      , state
      , country

    from source

)

select * from source