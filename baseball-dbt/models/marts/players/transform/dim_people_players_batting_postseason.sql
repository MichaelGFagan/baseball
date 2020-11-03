with batting as (

    select * from {{ ref('stg_lahman__batting') }} where is_postseason = TRUE

),

teams as (

    select * from {{ ref('stg_lahman__teams') }}

),

sums as (

    select
        b.person_id,
        {{ all_null_or_sum('b.games') }} as postseason_games_batted,
        {{ all_null_or_sum('b.plate_appearances') }} as postseason_plate_appearances,
        {{ all_null_or_sum('b.at_bats') }} as postseason_at_bats,
        {{ all_null_or_sum('b.runs') }} as postseason_runs,
        {{ all_null_or_sum('b.hits') }} as postseason_hits,
        {{ all_null_or_sum('b.doubles') }} as postseason_doubles,
        {{ all_null_or_sum('b.triples') }} as postseason_triples,
        {{ all_null_or_sum('b.home_runs') }} as postseason_home_runs,
        {{ all_null_or_sum('b.times_on_base') }} as tpostseason_imes_on_base,
        {{ all_null_or_sum('b.outs_made') }} as postseason_outs_made,
        {{ all_null_or_sum('b.extra_base_hits') }} as postseason_extra_base_hits,
        {{ all_null_or_sum('b.total_bases') }} as postseason_total_bases,
        {{ all_null_or_sum('b.runs_batted_in') }} as postseason_runs_batted_in,
        {{ all_null_or_sum('b.stolen_bases') }} as postseason_stolen_bases,
        {{ all_null_or_sum('b.caught_stealing') }} as postseason_caught_stealing,
        {{ all_null_or_sum('b.walks') }} as postseason_walks,
        {{ all_null_or_sum('b.strikeouts') }} as postseason_strikeouts,
        {{ all_null_or_sum('b.intentional_walks') }} as postseason_intentional_walks,
        {{ all_null_or_sum('b.hit_by_pitch') }} as postseason_hit_by_pitch,
        {{ all_null_or_sum('b.sacrifice_hits') }} as postseason_sacrifice_hits,
        {{ all_null_or_sum('b.sacrifice_flies') }} as postseason_sacrifice_flies,
        {{ all_null_or_sum('b.ground_into_double_plays') }} as postseason_ground_into_double_plays,
        {{ all_null_or_sum('b.on_base_denominator') }} as postseason_on_base_denominator

    from batting as b
    join teams as t using (year_id, team_id)

    group by 1

),

transformed as (

    select
        s.person_id,
        s.seasons as postseason_appearances,
        s.franchises as postseason_franchises,
        s.games as postseason_games,
        s.plate_appearances as postseason_plate_appearances,
        s.at_bats as postseason_at_bats,
        s.runs as postseason_runs,
        s.hits as postseason_hits,
        s.doubles as postseason_doubles,
        s.triples as postseason_triples,
        s.home_runs as postseason_home_runs,
        s.times_on_base as postseason_times_on_base,
        s.outs_made as postseason_outs_made,
        s.extra_base_hits as postseason_extra_base_hits,
        s.total_bases as postseason_total_bases,
        s.runs_batted_in as postseason_runs_batted_in,
        s.stolen_bases as postseason_stolen_bases,
        s.caught_stealing as postseason_caught_stealing,
        s.walks as postseason_walks,
        s.strikeouts as postseason_strikeouts,
        {{ no_divide_by_zero('s.hits', 's.at_bats', 3) }} as postseason_batting_average,
        {{ no_divide_by_zero('s.times_on_base', 's.on_base_denominator', 3) }} as postseason_on_base_percentage,
        {{ no_divide_by_zero('s.total_bases', 's.at_bats', 3) }} as postseason_slugging_percentage,
        {{ no_divide_by_zero('s.times_on_base', 's.on_base_denominator', 3) }} + {{ no_divide_by_zero('s.total_bases', 's.at_bats', 3) }} as postseason_on_base_plus_slugging,
        s.intentional_walks as postseason_intentional_walks,
        s.hit_by_pitch as postseason_hit_by_pitch,
        s.sacrifice_hits as postseason_sacrifice_hits,
        s.sacrifice_flies as postseason_sacrifice_flies,
        s.ground_into_double_plays as postseason_ground_into_double_plays,
        {{ no_divide_by_zero('s.home_runs', 's.plate_appearances', 3) }} as postseason_home_run_rate,
        {{ no_divide_by_zero('s.extra_base_hits', 's.plate_appearances', 3) }} as postseason_extra_base_hit_rate,
        {{ no_divide_by_zero('s.strikeouts', 's.plate_appearances', 3) }} as postseason_strikeout_rate,
        {{ no_divide_by_zero('s.walks', 's.plate_appearances', 3) }} as postseason_walk_rate,
        {{ no_divide_by_zero('s.walks', 's.strikeouts', 3) }} as postseason_walk_per_strikeout,
        {{ no_divide_by_zero('s.strikeouts', 's.walks', 3) }} as postseason_strikeout_per_walk

    from sums as s

)

select * from transformed
