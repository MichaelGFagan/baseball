with batting as (

    select * from {{ ref('stg_lahman__batting') }}

),

teams as (

    select * from {{ ref('stg_lahman__teams') }}

),

transformed as (

    select
        b.person_id,
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
        {{ no_divide_by_zero('b.hits', 'b.at_bats', 3) }} as batting_average,
        {{ no_divide_by_zero('b.times_on_base', 'b.on_base_denominator', 3) }} as on_base_percentage,
        {{ no_divide_by_zero('b.total_bases', 'b.at_bats', 3) }} as slugging_percentage,
        {{ no_divide_by_zero('b.times_on_base', 'b.on_base_denominator', 3) }} + {{ no_divide_by_zero('b.total_bases', 'b.at_bats', 3) }} as on_base_plus_slugging,
        b.intentional_walks,
        b.hit_by_pitch,
        b.sacrifice_hits,
        b.sacrifice_flies,
        b.ground_into_double_plays,
        {{ no_divide_by_zero('b.home_runs', 'b.plate_appearances', 3) }} as home_run_rate,
        {{ no_divide_by_zero('b.extra_base_hits', 'b.plate_appearances', 3) }} as extra_base_hit_rate,
        {{ no_divide_by_zero('b.strikeouts', 'b.plate_appearances', 3) }} as strikeout_rate,
        {{ no_divide_by_zero('b.walks', 'b.plate_appearances', 3) }} as walk_rate,
        {{ no_divide_by_zero('b.walks', 'b.strikeouts', 3) }} as walk_per_strikeout,
        {{ no_divide_by_zero('b.strikeouts', 'b.walks', 3) }} as strikeout_per_walk

    from batting as b
    left join teams as t using (year_id, team_id)

)

select * from transformed
