with metric_values as (

    {{ daily_metric_values() }}

),

weeks_back as (

    {{ number_spine(var('anomaly_comparison_weeks') - 1) }}

),

ranked as (

    select
        a.brand,
        a.metric,
        a.date_day,
        earlier.value                                                           as earlier_value,
        row_number() over (
            partition by a.brand, a.metric, a.date_day order by earlier.value
        )                                                                       as position,
        count(*) over (partition by a.brand, a.metric, a.date_day)              as earlier_values
    from {{ ref('exec_kpi_anomalies') }} as a
    cross join weeks_back as w
    inner join metric_values as earlier
        on a.brand = earlier.brand
        and a.metric = earlier.metric
        and earlier.date_day = dateadd(day, -7 * (w.n + 1), a.date_day)::date

),

by_rank as (

    select
        brand,
        metric,
        date_day,
        max(earlier_values)                                                     as earlier_values,
        avg(earlier_value)                                                      as median_by_rank
    from ranked
    where position in (floor((earlier_values + 1) / 2), floor((earlier_values + 2) / 2))
    group by brand, metric, date_day

)

select
    a.brand,
    a.metric,
    a.date_day,
    a.comparison_days,
    r.earlier_values,
    a.baseline_median,
    r.median_by_rank
from {{ ref('exec_kpi_anomalies') }} as a
left join by_rank as r
    on a.brand = r.brand and a.metric = r.metric and a.date_day = r.date_day
where a.comparison_days <> coalesce(r.earlier_values, 0)
   or (r.median_by_rank is null) <> (a.baseline_median is null)
   or abs(a.baseline_median - r.median_by_rank) > 0.000001
