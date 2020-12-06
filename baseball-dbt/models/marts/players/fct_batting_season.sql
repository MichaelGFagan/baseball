with batting as (

    select * from {{ ref('stg_lahman__batting') }}

),

sums as (

    select
        b.person_id,
        b.year_id,
        b.is_postseason,
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
        sum(ifnull(b.intentional_walks, 0)) as intentional_walks,
        sum(ifnull(b.hit_by_pitches, 0)) as hit_by_pitches,
        sum(ifnull(b.sacrifice_hits, 0)) as sacrifice_hits,
        sum(ifnull(b.sacrifice_flies, 0)) as sacrifice_flies,
        sum(ifnull(b.ground_into_double_plays, 0)) as ground_into_double_plays,
        sum(ifnull(b.on_base_denominator, 0)) as on_base_denominator

    from batting as b

    group by 1, 2, 3

),

transformed as (

    select
        s.person_id,
        s.year_id,
        s.is_postseason,
        s.teams,
        s.games,
        s.plate_appearances,
        s.at_bats,
        s.runs,
        s.hits,
        s.doubles,
        s.triples,
        s.home_runs,
        s.times_on_base,
        s.outs_made,
        s.extra_base_hits,
        s.total_bases,
        s.runs_batted_in,
        s.stolen_bases,
        s.caught_stealing,
        s.walks,
        s.strikeouts,
        {{ no_divide_by_zero('s.hits', 's.at_bats', 3) }} as batting_average,
        {{ no_divide_by_zero('s.times_on_base', 's.on_base_denominator', 3) }} as on_base_percentage,
        {{ no_divide_by_zero('s.total_bases', 's.at_bats', 3) }} as slugging_percentage,
        {{ no_divide_by_zero('s.times_on_base', 's.on_base_denominator', 3) }} + {{ no_divide_by_zero('s.total_bases', 's.at_bats', 3) }} as on_base_plus_slugging,
        s.intentional_walks,
        s.hit_by_pitches,
        s.sacrifice_hits,
        s.sacrifice_flies,
        s.ground_into_double_plays,
        {{ no_divide_by_zero('s.home_runs', 's.plate_appearances', 3) }} as home_run_rate,
        {{ no_divide_by_zero('s.extra_base_hits', 's.plate_appearances', 3) }} as extra_base_hit_rate,
        {{ no_divide_by_zero('s.strikeouts', 's.plate_appearances', 3) }} as strikeout_rate,
        {{ no_divide_by_zero('s.walks', 's.plate_appearances', 3) }} as walk_rate,
        {{ no_divide_by_zero('s.walks', 's.strikeouts', 3) }} as walk_per_strikeout,
        {{ no_divide_by_zero('s.strikeouts', 's.walks', 3) }} as strikeout_per_walk

    from sums as s

)

select * from transformed
