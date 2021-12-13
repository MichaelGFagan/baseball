 with source as (

    select * from {{ source('lahman', 'college_playing') }}

),

chadwick as (

    select * from {{ ref('util_chadwick__register') }}

),

renamed as (

    select
        playerid as lahman_id
      , yearid as year_id
      , schoolid as school_id

    from source

),

final as (

    select
        {{ dbt_utils.surrogate_key(['chadwick.person_id', 'renamed.school_id', 'renamed.year_id']) }} as player_college_season_id
      , chadwick.person_id
      , renamed.*

    from renamed
    left join chadwick
        on renamed.lahman_id = chadwick.lahman_id

)

select * from final
