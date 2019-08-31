with batting as (

    select * from {{ ref('stg_lahman__batting') }}

)

select
    b.person_id,
    count(distinct b.year_id) as seasons,
    -- count(distinct )
    sum(b.games) as games,
    sum(b.plate_appearances) as plate_appearances,
    sum(b.at_bats) as at_bats,
    sum(b.runs) as runs,
    sum(b.hits) as hits,
    sum(b.doubles) as doubles,
    sum(b.triples) as triples,
    sum(b.home_runs) as home_runs,
    sum(b.times_on_base) as times_on_base,
    sum(b.outs_made) as outs_made,
    sum(b.extra_base_hits) as extra_base_hits,
    sum(b.total_bases) as total_bases,
    sum(b.runs_batted_in) as runs_batted_in,
    sum(b.stolen_bases) as stolen_bases,
    sum(b.caught_stealing) as caught_stealing,
    sum(b.walks) as walks,
    sum(b.strikeouts) as strikeouts,
    case
        when sum(b.at_bats) = 0 then round(0, 3)
        else round(sum(b.hits) / sum(b.at_bats), 3)
    end as batting_average,
    case
        when sum(b.on_base_denominator) = 0 then round(0, 3)
        else round(sum(b.times_on_base) / sum(b.on_base_denominator), 3)
    end as on_base_percentage,
    case
        when sum(b.at_bats) = 0 then round(0, 3)
        else round(sum(b.total_bases) / sum(b.at_bats), 3)
    end as slugging_percentage,
    case
        when sum(b.on_base_denominator) = 0 then 0
        else round(sum(b.times_on_base) / sum(b.on_base_denominator), 3)
    end +
    case
        when sum(b.at_bats) = 0 then 0
        else round(sum(b.total_bases) / sum(b.at_bats), 3)
    end as on_base_plus_slugging,
    sum(b.intentional_walks) as intentional_walks,
    sum(b.hit_by_pitch) as hit_by_pitch,
    sum(b.sacrifice_hits) as sacrifice_hits,
    sum(b.sacrifice_flies) as sacrifice_flies,
    sum(b.ground_into_double_plays) as ground_into_double_plays,
    case
        when sum(b.plate_appearances) = 0 then round(0, 3)
        else round(sum(b.home_runs) / sum(b.plate_appearances), 3)
    end as home_run_rate,
    case
        when sum(b.plate_appearances) = 0 then round(0, 3)
        else round(sum(b.extra_base_hits) / sum(b.plate_appearances), 3)
    end as extra_base_hit_rate,
    case
        when sum(b.plate_appearances) = 0 then round(0, 3)
        else round(sum(b.strikeouts) / sum(b.plate_appearances), 3)
    end as strikeout_rate,
    case
        when sum(b.plate_appearances) = 0 then round(0, 3)
        else round(sum(b.walks) / sum(b.plate_appearances), 3)
    end as walk_rate,
    case
        when sum(b.strikeouts) = 0 then round(0, 2)
        else round(sum(b.walks) / sum(b.strikeouts), 2)
    end as walk_to_strikeout_ratio,
    case
        when sum(b.walks) = 0 then round(0, 2)
        else round(sum(b.strikeouts) / sum(b.walks), 2)
    end as strikeout_to_walk_ratio

from batting as b

group by 1
