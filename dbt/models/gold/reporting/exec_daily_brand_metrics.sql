{#
    One row per brand per day, from the first order to the as-of date. Feeds the executive
    report's time charts: weekly revenue and contribution margin, weekly new customers, the
    monthly new / cancelled / net subscriptions chart and weekly ad spend. Every column adds up
    across days and brands, so a chart only needs SUM and a time grain. Revenue is gross sales minus
    the refunds issued that day, and contribution margin also subtracts the cost of that day's
    orders. Amounts are in USD at constant-currency rates.
#}
with bounds as (

    select min(order_date) as first_day, {{ as_of_date() }} as as_of_date
    from {{ ref('fct_orders') }}

),

days as (

    select dateadd(day, s.n, b.first_day) as date_day, b.as_of_date
    from bounds as b
    cross join ({{ number_spine(10000) }}) as s
    where dateadd(day, s.n, b.first_day) <= b.as_of_date

),

orders_by_day as (

    -- Gross sales, margin and new subscriptions in one pass over the orders
    select
        brand,
        order_date,
        sum(iff(is_revenue_order, gross_sales_usd, 0))           as gross_sales_usd,
        sum(iff(is_revenue_order, margin_before_refunds_usd, 0)) as margin_before_refunds_usd,
        count(iff(is_revenue_order, 1, null))                    as revenue_orders,
        count(iff(is_paid_subscription, 1, null))                as new_subscriptions
    from {{ ref('fct_orders') }}
    group by brand, order_date

),

refunds_by_day as (

    select brand, issued_date, sum(refund_amount_usd) as refunds_usd
    from {{ ref('fct_refunds') }}
    where reduces_revenue
    group by brand, issued_date

),

brands as (

    select b.brand, b.brand_name
    from {{ ref('brands') }} as b
    where b.brand in (select brand from orders_by_day)

),

new_customers as (

    select brand, first_order_date, count(*) as new_customers
    from {{ ref('dim_customers') }}
    group by brand, first_order_date

),

canceled_subscriptions as (

    select brand, cancel_date, count(*) as canceled_subscriptions
    from {{ ref('fct_orders') }}
    where is_paid_subscription and is_subscription_canceled and cancel_date is not null
    group by brand, cancel_date

),

ad_spend as (

    select brand, spend_date, sum(spend_usd) as ad_spend_usd
    from {{ ref('fct_marketing_spend') }}
    group by brand, spend_date

)

-- Each column is cast to its contract type (_reporting.yml)
select
    b.brand::varchar                                                   as brand,
    b.brand_name::varchar                                              as brand_name,
    d.date_day::date                                                   as date_day,
    coalesce(o.gross_sales_usd, 0)::float                              as gross_sales_usd,
    coalesce(rf.refunds_usd, 0)::float                                 as refunds_usd,
    (coalesce(o.gross_sales_usd, 0) - coalesce(rf.refunds_usd, 0))::float
                                                                       as revenue_usd,
    (coalesce(o.margin_before_refunds_usd, 0) - coalesce(rf.refunds_usd, 0))::float
                                                                       as contribution_margin_usd,
    coalesce(o.revenue_orders, 0)::int                                 as revenue_orders,
    coalesce(nc.new_customers, 0)::int                                 as new_customers,
    coalesce(o.new_subscriptions, 0)::int                              as new_subscriptions,
    -- Negative on purpose: the report draws cancellations below the axis
    (-coalesce(cs.canceled_subscriptions, 0))::int                     as canceled_subscriptions,
    (coalesce(o.new_subscriptions, 0) - coalesce(cs.canceled_subscriptions, 0))::int
                                                                       as net_subscriptions,
    coalesce(sp.ad_spend_usd, 0)::float                                as ad_spend_usd
from brands as b
cross join days as d
left join orders_by_day as o
    on b.brand = o.brand and d.date_day = o.order_date
left join refunds_by_day as rf
    on b.brand = rf.brand and d.date_day = rf.issued_date
left join new_customers as nc
    on b.brand = nc.brand and d.date_day = nc.first_order_date
left join canceled_subscriptions as cs
    on b.brand = cs.brand and d.date_day = cs.cancel_date
left join ad_spend as sp
    on b.brand = sp.brand and d.date_day = sp.spend_date
