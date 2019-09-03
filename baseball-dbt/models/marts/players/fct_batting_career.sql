with batting as (

    select * from {{ ref('stg_lahman__batting') }}

)

select
    b.person_id,
    count(distinct b.year_id) as seasons,
    count(distinct b.team_id) as teams,
    sum(ifnull(b.games, 0)) as games,
    sum(ifnull(b.plate_appearances, 0)) as plate_appearances,
    sum(ifnull(b.at_bats, 0)) as at_bats,
    sum(ifnull(b.runs, 0)) as runs,
    sum(ifnull(b.hits, 0)) as hits,
    sum(ifnull(b.doubles, 0)) as doubles,
    sum(ifnull(b.triples, 0)) as triples,
    sum(ifnull(b.home_runs, 0)) as home_runs,
    sum(ifnull(b.times_on_base, 0)) as times_on_base,
    sum(ifnull(b.outs_made, 0)) as outs_made,
    sum(ifnull(b.extra_base_hits, 0)) as extra_base_hits,
    sum(ifnull(b.total_bases, 0)) as total_bases,
    sum(ifnull(b.runs_batted_in, 0)) as runs_batted_in,
    sum(ifnull(b.stolen_bases, 0)) as stolen_bases,
    sum(ifnull(b.caught_stealing, 0)) as caught_stealing,
    sum(ifnull(b.walks, 0)) as walks,
    sum(ifnull(b.strikeouts, 0)) as strikeouts,
    case
        when sum(ifnull(b.at_bats, 0)) = 0 then round(0, 3)
        else round(sum(ifnull(b.hits, 0)) / sum(ifnull(b.at_bats, 0)), 3)
    end as batting_average,
    case
        when sum(ifnull(b.on_base_denominator, 0)) = 0 then round(0, 3)
        else round(sum(ifnull(b.times_on_base, 0)) / sum(ifnull(b.on_base_denominator, 0)), 3)
    end as on_base_percentage,
    case
        when sum(ifnull(b.at_bats, 0)) = 0 then round(0, 3)
        else round(sum(ifnull(b.total_bases, 0)) / sum(ifnull(b.at_bats, 0)), 3)
    end as slugging_percentage,
    case
        when sum(ifnull(b.on_base_denominator, 0)) = 0 then 0
        else round(sum(ifnull(b.times_on_base, 0)) / sum(ifnull(b.on_base_denominator, 0)), 3)
    end +
    case
        when sum(ifnull(b.at_bats, 0)) = 0 then 0
        else round(sum(ifnull(b.total_bases, 0)) / sum(ifnull(b.at_bats, 0)), 3)
    end as on_base_plus_slugging,
    sum(ifnull(b.intentional_walks, 0)) as intentional_walks,
    sum(ifnull(b.hit_by_pitch, 0)) as hit_by_pitch,
    sum(ifnull(b.sacrifice_hits, 0)) as sacrifice_hits,
    sum(ifnull(b.sacrifice_flies, 0)) as sacrifice_flies,
    sum(ifnull(b.ground_into_double_plays, 0)) as ground_into_double_plays,
    case
        when sum(ifnull(b.plate_appearances, 0)) = 0 then round(0, 3)
        else round(sum(ifnull(b.home_runs, 0)) / sum(ifnull(b.plate_appearances, 0)), 3)
    end as home_run_rate,
    case
        when sum(ifnull(b.plate_appearances, 0)) = 0 then round(0, 3)
        else round(sum(ifnull(b.extra_base_hits, 0)) / sum(ifnull(b.plate_appearances, 0)), 3)
    end as extra_base_hit_rate,
    case
        when sum(ifnull(b.plate_appearances, 0)) = 0 then round(0, 3)
        else round(sum(ifnull(b.strikeouts, 0)) / sum(ifnull(b.plate_appearances, 0)), 3)
    end as strikeout_rate,
    case
        when sum(ifnull(b.plate_appearances, 0)) = 0 then round(0, 3)
        else round(sum(ifnull(b.walks, 0)) / sum(ifnull(b.plate_appearances, 0)), 3)
    end as walk_rate,
    case
        when sum(ifnull(b.strikeouts, 0)) = 0 then round(0, 2)
        else round(sum(ifnull(b.walks, 0)) / sum(ifnull(b.strikeouts, 0)), 2)
    end as walk_per_strikeout,
    case
        when sum(ifnull(b.walks, 0)) = 0 then round(0, 2)
        else round(sum(ifnull(b.strikeouts, 0)) / sum(ifnull(b.walks, 0)), 2)
    end as strikeout_per_walk

from batting as b

group by 1
