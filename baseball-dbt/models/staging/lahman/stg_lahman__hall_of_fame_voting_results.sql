 with source as (

    select * from {{ source('lahman', 'hall_of_fame') }}

),

chadwick as (

    select * from {{ ref('util_chadwick__register') }}

),

renamed as (

    select
        playerid as lahman_id
      , yearid as year_id
      , votedby as voting_body
      , ballots as total_ballots_cast
      , needed as votes_needed
      , votes as votes_received
      , case
            when inducted = 'Y' then true
            when inducted = 'N' then false
        end as was_inducted
      , category
      , needed_note as note
    
    from source

),

final as (

    select
        {{ dbt_utils.surrogate_key(['chadwick.person_id', 'renamed.year_id', 'renamed.voting_body']) }} as hall_of_fame_voting_result_id
      , chadwick.person_id
      , renamed.*

    from renamed
    left join chadwick
        on renamed.lahman_id = chadwick.lahman_id

)

select * from final
