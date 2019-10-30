with batting as (

    select * from {{ ref('stg_lahman__batting') }}

),

teams as (

    select * from {{ ref('stg_lahman__teams') }}

)

select
    b.baseball_reference_id,
    b.year_id,
    b.stint,
    b.team_id,
    t.franchise_id,
    b.league_id,
    b.games,
    b.plate_appearances,
    b.at_bats,
    b.runs,
    b.hits,
    b.doubles,
    b.triples,
    b.home_runs,
    b.times_on_base,
    b.outs_made,
    b.extra_base_hits,
    b.total_bases,
    b.runs_batted_in,
    b.stolen_bases,
    b.caught_stealing,
    b.walks,
    b.strikeouts,
    case
        when b.at_bats = 0 then round(0, 3)
        else round(b.hits / b.at_bats, 3)
    end as batting_average,
    case
        when b.on_base_denominator = 0 then round(0, 3)
        else round(b.times_on_base / b.on_base_denominator, 3)
    end as on_base_percentage,
    case
        when b.at_bats = 0 then round(0, 3)
        else round(b.total_bases / b.at_bats, 3)
    end as slugging_percentage,
    case
        when b.on_base_denominator = 0 then 0
        else round(b.times_on_base / b.on_base_denominator, 3)
    end +
    case
        when b.at_bats = 0 then 0
        else round(b.total_bases / b.at_bats, 3)
    end as on_base_plus_slugging,
    b.intentional_walks,
    b.hit_by_pitch,
    b.sacrifice_hits,
    b.sacrifice_flies,
    b.ground_into_double_plays,
    case
        when b.plate_appearances = 0 then round(0, 3)
        else round(b.home_runs / b.plate_appearances, 3)
    end as home_run_rate,
    case
        when b.plate_appearances = 0 then round(0, 3)
        else round(b.extra_base_hits / b.plate_appearances, 3)
    end as extra_base_hit_rate,
    case
        when b.plate_appearances = 0 then round(0, 3)
        else round(b.strikeouts / b.plate_appearances, 3)
    end as strikeout_rate,
    case
        when b.plate_appearances = 0 then round(0, 3)
        else round(b.walks / b.plate_appearances, 3)
    end as walk_rate,
    case
        when b.strikeouts = 0 then round(0, 2)
        else round(b.walks / b.strikeouts, 2)
    end as walk_per_strikeout,
    case
        when b.walks = 0 then round(0, 2)
        else round(b.strikeouts / b.walks, 2)
    end as strikeout_per_walk

from batting as b
left join teams as t using (year_id, team_id)
