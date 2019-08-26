with batting as (

    select * from {{ ref('stg_lahman__batting') }}

)

select
    b.person_id,
    b.year_id,
    b.stint,
    b.team_id,
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
    round(b.hits / b.at_bats, 3) as batting_average,
    round(b.times_on_base / b.on_base_denominator, 3) as on_base_percentage,
    round(b.total_bases / b.at_bats, 3) as slugging_percentage,
    round(b.times_on_base / b.on_base_denominator, 3) + round(b.total_bases / b.at_bats, 3) as on_base_plus_slugging,
    b.intentional_walks,
    b.hit_by_pitch,
    b.sacrifice_hits,
    b.sacrifice_flies,
    b.ground_into_double_plays,
    round(b.home_runs / b.plate_appearances, 3) as home_run_rate,
    round(b.extra_base_hits / b.plate_appearances, 3) as extra_base_hit_rate,
    round(b.strikeouts / b.plate_appearances, 3) as strikeout_rate,
    round(b.walks / b.plate_appearances, 3) as walk_rate,
    round(b.walks / b.strikeouts, 1) as walk_to_strikeout_ratio,
    round(b.strikeouts / b.walks, 1) as strikeout_to_walk_ratio

from batting as b
