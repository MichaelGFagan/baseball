with appearances as (

    select
        {{ dbt_utils.star(from = ref('stg_lahman__appearances'), except = ['baseball_reference_id']) }}

    from {{ ref('stg_lahman__appearances') }}

)

select * from appearances
