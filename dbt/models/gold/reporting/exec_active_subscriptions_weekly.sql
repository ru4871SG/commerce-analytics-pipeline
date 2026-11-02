{#
    Active subscriptions per brand at the end of each week (Monday-start weeks; the current
    week is measured at the as-of date). A paid subscription order is active from its order date
    until its cancellation date. Snapshots do not add up over time, so this table is already at
    the chart's grain: one value per brand and week, which SUM combines across brands.
#}
with subscriptions as (

    select
        brand,
        order_date                                                         as started_on,
        -- Cancelled with no date, or before it started: never counted as active
        iff(is_subscription_canceled, greatest(order_date, coalesce(cancel_date, order_date)), null)
                                                                           as ended_on
    from {{ ref('fct_orders') }}
    where is_paid_subscription

),

bounds as (

    select min(started_on) as first_day, {{ as_of_date() }} as as_of_date
    from subscriptions

),

days as (

    select dateadd(day, s.n, b.first_day) as date_day, b.as_of_date
    from bounds as b
    cross join ({{ number_spine(10000) }}) as s
    where dateadd(day, s.n, b.first_day) <= b.as_of_date

),

starts as (

    select brand, started_on, count(*) as started
    from subscriptions
    group by brand, started_on

),

ends as (

    select brand, ended_on, count(*) as ended
    from subscriptions
    where ended_on is not null
    group by brand, ended_on

),

daily as (

    select
        b.brand,
        d.date_day,
        d.as_of_date,
        sum(coalesce(s.started, 0) - coalesce(e.ended, 0)) over (
            partition by b.brand order by d.date_day rows between unbounded preceding and current row
        ) as active_subscriptions
    from (select distinct brand from subscriptions) as b
    cross join days as d
    left join starts as s
        on b.brand = s.brand and d.date_day = s.started_on
    left join ends as e
        on b.brand = e.brand and d.date_day = e.ended_on

)

select
    daily.brand,
    brands.brand_name,
    date_trunc(week, daily.date_day)       as week_start,
    daily.date_day                         as snapshot_date,
    daily.active_subscriptions
from daily
inner join {{ ref('brands') }} as brands
    on daily.brand = brands.brand
where daily.date_day = least(dateadd(day, 6, date_trunc(week, daily.date_day)), daily.as_of_date)
