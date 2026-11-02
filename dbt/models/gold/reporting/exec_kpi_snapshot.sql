{#
    One row per brand with every number card of the executive report, measured at the as-of
    date. Counts and amounts add up across brands (SUM). Period churn adds up as its two counts
    (SUM of the lost / SUM of the active at the start). The cancelled-within-N-days rates are
    averaged across brands (AVG). Amounts are in USD at
    constant-currency rates, so they add up across brands in different currencies.
    Revenue is gross sales minus the refunds issued in the window, whenever the order was placed.
    Contribution margin also subtracts the cost of the window's orders.
    Windows: MTD = first of the month to the as-of date; PMTD = the same span one month earlier;
    YTD / PYTD likewise for the year; LnD = the n days ending on the as-of date.
#}
{%- set windows = [
    ('mtd', 'd.month_start', 'd.as_of_date'),
    ('pmtd', 'd.prev_month_start', 'd.prev_month_as_of'),
    ('ytd', 'd.year_start', 'd.as_of_date'),
    ('pytd', 'd.prev_year_start', 'd.prev_year_as_of'),
] %}
with as_of as (

    select
        {{ as_of_date() }}                                          as as_of_date

),

dates as (

    select
        as_of_date,
        date_trunc(month, as_of_date)                               as month_start,
        dateadd(month, -1, as_of_date)                              as prev_month_as_of,
        date_trunc(month, dateadd(month, -1, as_of_date))           as prev_month_start,
        date_trunc(year, as_of_date)                                as year_start,
        dateadd(year, -1, as_of_date)                               as prev_year_as_of,
        date_trunc(year, dateadd(year, -1, as_of_date))             as prev_year_start
    from as_of

),

orders as (

    -- Gross sales, margin and net subscriptions in one pass over the orders. A brand is reported
    -- when it has any order.
    select
        o.brand,
        {%- for name, first_day, last_day in windows %}
        sum(iff(o.is_revenue_order and o.order_date between {{ first_day }} and {{ last_day }},
                o.gross_sales_usd, 0))                                                       as gross_sales_{{ name }}_usd,
        sum(iff(o.is_revenue_order and o.order_date between {{ first_day }} and {{ last_day }},
                o.margin_before_refunds_usd, 0))                                             as margin_before_refunds_{{ name }}_usd,
        {%- endfor %}
        {%- for days in [30, 60, 90, 180] %}
        count_if(o.is_paid_subscription
                 and o.order_date > dateadd(day, -{{ days }}, d.as_of_date) and o.order_date <= d.as_of_date)
        - count_if(o.is_paid_subscription and o.is_subscription_canceled
                   and o.cancel_date > dateadd(day, -{{ days }}, d.as_of_date) and o.cancel_date <= d.as_of_date)
                                                                                             as net_subscriptions_l{{ days }}d{{ "," if not loop.last }}
        {%- endfor %}
    from {{ ref('fct_orders') }} as o
    cross join dates as d
    group by o.brand

),

refunds as (

    select
        r.brand,
        {%- for name, first_day, last_day in windows %}
        sum(iff(r.issued_date between {{ first_day }} and {{ last_day }}, r.refund_amount_usd, 0))
                                                                                             as refunds_{{ name }}_usd{{ "," if not loop.last }}
        {%- endfor %}
    from {{ ref('fct_refunds') }} as r
    cross join dates as d
    where r.reduces_revenue
    group by r.brand

),

new_customers as (

    select
        c.brand,
        count_if(c.first_order_date between d.month_start and d.as_of_date)                  as new_customers_mtd,
        count_if(c.first_order_date between d.prev_month_start and d.prev_month_as_of)       as new_customers_pmtd,
        {%- for days in [30, 60, 90] %}
        count_if(c.first_order_date > dateadd(day, -{{ days }}, d.as_of_date)
                 and c.first_order_date <= d.as_of_date)                                     as new_customers_l{{ days }}d{{ "," if not loop.last }}
        {%- endfor %}
    from {{ ref('dim_customers') }} as c
    cross join dates as d
    group by c.brand

),

subscriptions as (

    -- Started and ended on the days exec_active_subscriptions_weekly counts them
    select
        brand,
        order_date                                                                           as started_on,
        iff(is_subscription_canceled, greatest(order_date, coalesce(cancel_date, order_date)), null)
                                                                                             as ended_on
    from {{ ref('fct_orders') }}
    where is_paid_subscription

),

period_churn as (

    -- Of those active N days ago, how many have been lost since. A subscription that started
    -- inside the window is not in the base, so its cancellation does not count as lost.
    select
        s.brand,
        {%- for days in [30, 60, 90] %}
        {%- set active_at_start %}s.started_on <= dateadd(day, -{{ days }}, d.as_of_date)
                 and (s.ended_on is null or s.ended_on > dateadd(day, -{{ days }}, d.as_of_date)){% endset %}
        count_if({{ active_at_start }})                                                      as active_subscriptions_start_l{{ days }}d,
        count_if({{ active_at_start }}
                 and s.ended_on <= d.as_of_date)                                             as lost_subscriptions_l{{ days }}d{{ "," if not loop.last }}
        {%- endfor %}
    from subscriptions as s
    cross join dates as d
    group by s.brand

),

canceled_within as (

    select
        brand,
        max(iff(day_offset = 30, canceled_rate, null))  as canceled_within_30d_rate,
        max(iff(day_offset = 60, canceled_rate, null))  as canceled_within_60d_rate,
        max(iff(day_offset = 90, canceled_rate, null))  as canceled_within_90d_rate
    from {{ ref('exec_subscription_survival') }}
    group by brand

)

select
    b.brand,
    b.brand_name,
    d.as_of_date,
    {%- for name, first_day, last_day in windows %}
    coalesce(o.gross_sales_{{ name }}_usd, 0) - coalesce(rf.refunds_{{ name }}_usd, 0)
                                                    as revenue_{{ name }}_usd,
    coalesce(o.margin_before_refunds_{{ name }}_usd, 0) - coalesce(rf.refunds_{{ name }}_usd, 0)
                                                    as contribution_margin_{{ name }}_usd,
    {%- endfor %}
    coalesce(nc.new_customers_mtd, 0)               as new_customers_mtd,
    coalesce(nc.new_customers_pmtd, 0)              as new_customers_pmtd,
    coalesce(nc.new_customers_l30d, 0)              as new_customers_l30d,
    coalesce(nc.new_customers_l60d, 0)              as new_customers_l60d,
    coalesce(nc.new_customers_l90d, 0)              as new_customers_l90d,
    coalesce(o.net_subscriptions_l30d, 0)::int      as net_subscriptions_l30d,
    coalesce(o.net_subscriptions_l60d, 0)::int      as net_subscriptions_l60d,
    coalesce(o.net_subscriptions_l90d, 0)::int      as net_subscriptions_l90d,
    coalesce(o.net_subscriptions_l180d, 0)::int     as net_subscriptions_l180d,
    {%- for days in [30, 60, 90] %}
    coalesce(pc.active_subscriptions_start_l{{ days }}d, 0)::int
                                                    as active_subscriptions_start_l{{ days }}d,
    coalesce(pc.lost_subscriptions_l{{ days }}d, 0)::int
                                                    as lost_subscriptions_l{{ days }}d,
    pc.lost_subscriptions_l{{ days }}d / nullif(pc.active_subscriptions_start_l{{ days }}d, 0)
                                                    as churn_rate_l{{ days }}d,
    {%- endfor %}
    cw.canceled_within_30d_rate,
    cw.canceled_within_60d_rate,
    cw.canceled_within_90d_rate
from {{ ref('brands') }} as b
cross join dates as d
inner join orders as o
    on b.brand = o.brand
left join refunds as rf
    on b.brand = rf.brand
left join new_customers as nc
    on b.brand = nc.brand
left join period_churn as pc
    on b.brand = pc.brand
left join canceled_within as cw
    on b.brand = cw.brand
