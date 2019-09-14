with managers as (

    select * from {{ ref('stg_lahman__managers') }}

),

teams as (

    select * from {{ ref('stg_lahman__teams') }}

),

sums as (

    select
        m.person_id,
        count(distinct m.year_id) as seasons,
        min(m.year_id) as debut_year,
        max(m.year_id) as final_year,
        count(distinct t.franchise_id) count_franchises_managed,
        sum(m.games) as games,
        sum(m.wins) as wins,
        sum(m.losses) as losses

    from managers as m
    join teams as t using (team_id, year_id)

    group by 1

)

select * from sums
