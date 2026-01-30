with source as (
    select * from {{ ref('reducoes_h1_2025') }}
    where initiative is not null
),

year_to_end as (
    select t.date as start_date
    from (
        select sequence(current_date - interval '2' day, timestamp '2025-12-31' , interval '1' day) dates
    ), unnest(dates) as t(date)
),

seven_day_avg as (
    select initiative,
            sum(baseline_cost)/7 as baseline_cost,
            sum(actual_cost)/7 as actual_cost
    from source
    where start_date < current_date - interval '4' day
    and start_date >= current_date - interval '11' day
    group by 1
)

select start_date,
        initiative,
        sum(baseline_cost) as baseline_cost,
        sum(actual_cost) as actual_cost,
        'Realizado' as type
from source
where start_date < current_date - interval '2' day
and start_date >= timestamp '2025-01-01'
group by 1,2

union all

select y.start_date,
        s.initiative,
        s.baseline_cost,
        s.actual_cost,
        'Projeção' as type
from seven_day_avg s
cross join year_to_end y
order by 1,2 desc
