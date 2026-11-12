{#
    Which of a brand's daily numbers are unusual for that brand on that weekday. One row per
    brand, metric and day, for the trailing var('anomaly_window_days') days that end on the as-of
    date. Feeds the dashboard's table of the as-of day and the sentences the digest leads with.

    A day is compared with the same weekday of the weeks before (up to
    var('anomaly_comparison_weeks') of them), because shops sell differently on a Monday and on a
    Saturday:
      baseline_median   the median of those earlier values: the usual value for that weekday
      spread            1.4826 x the median distance of those values from their median (MAD); the
                        factor makes it read like a standard deviation
      robust_z          (value - baseline_median) / spread, to two decimals. Null when the spread
                        is zero: every earlier value was the same, so the distance has no scale
      is_anomaly        enough earlier weeks, a robust z-score of at least the threshold, and a
                        difference of at least the metric's minimum. With a spread of zero the
                        minimum alone decides. The z-score counts as the table shows it, rounded,
                        so a row can be checked from its own columns
    The median and MAD are used, not the mean and standard deviation, so that one earlier
    unusual day does not hide the next. The rule's numbers are dbt vars (dbt_project.yml).
    Cancellations are positive here. Amounts are in USD at constant-currency rates.
#}
with metric_values as (

    {{ daily_metric_values() }}

),

days as (

    -- The days the table holds
    select brand, brand_name, date_day, metric, value
    from metric_values
    where date_day > dateadd(day, -{{ var('anomaly_window_days') }}, {{ as_of_date() }})

),

weeks_back as (

    {{ number_spine(var('anomaly_comparison_weeks') - 1) }}

),

comparisons as (

    -- The same weekday of each earlier week that the data has
    select
        d.brand,
        d.metric,
        d.date_day,
        earlier.value                       as earlier_value
    from days as d
    cross join weeks_back as w
    inner join metric_values as earlier
        on d.brand = earlier.brand
        and d.metric = earlier.metric
        and earlier.date_day = dateadd(day, -7 * (w.n + 1), d.date_day)::date

),

baselines as (

    select
        brand,
        metric,
        date_day,
        count(*)                            as comparison_days,
        median(earlier_value)               as baseline_median
    from comparisons
    group by brand, metric, date_day

),

spreads as (

    select
        c.brand,
        c.metric,
        c.date_day,
        1.4826 * median(abs(c.earlier_value - b.baseline_median)) as spread
    from comparisons as c
    inner join baselines as b
        on c.brand = b.brand and c.metric = b.metric and c.date_day = b.date_day
    group by c.brand, c.metric, c.date_day

),

scored as (

    select
        d.brand,
        d.brand_name,
        d.date_day,
        d.metric,
        d.value,
        b.baseline_median,
        s.spread,
        d.value - b.baseline_median         as difference,
        round((d.value - b.baseline_median) / nullif(s.spread, 0), 2) as robust_z,
        coalesce(b.comparison_days, 0)      as comparison_days,
        case d.metric
            {%- for metric, minimum in var('anomaly_min_difference').items() %}
            when '{{ metric }}' then {{ minimum }}
            {%- endfor %}
        end                                 as min_difference
    from days as d
    left join baselines as b
        on d.brand = b.brand and d.metric = b.metric and d.date_day = b.date_day
    left join spreads as s
        on d.brand = s.brand and d.metric = s.metric and d.date_day = s.date_day

)

-- Each column is cast to its contract type (_reporting.yml)
select
    brand::varchar                                                      as brand,
    brand_name::varchar                                                 as brand_name,
    date_day::date                                                      as date_day,
    metric::varchar                                                     as metric,
    value::float                                                        as value,
    baseline_median::float                                              as baseline_median,
    spread::float                                                       as spread,
    robust_z::number(18, 2)                                             as robust_z,
    (case when difference > 0 then 'above' when difference < 0 then 'below' end)::varchar
                                                                        as direction,
    coalesce(
        comparison_days >= {{ var('anomaly_min_comparison_days') }}
        and abs(difference) >= min_difference
        and (spread = 0 or abs(robust_z) >= {{ var('anomaly_z_threshold') }}),
        false
    )::boolean                                                          as is_anomaly,
    (date_day = {{ as_of_date() }})::boolean                            as is_as_of_day,
    comparison_days::int                                                as comparison_days
from scored
