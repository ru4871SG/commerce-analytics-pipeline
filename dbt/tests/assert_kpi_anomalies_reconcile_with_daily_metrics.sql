with reported as (

    select
        brand,
        date_day,
        count(*)                                                        as metrics,
        max(iff(metric = 'revenue_usd', value, null))                   as revenue_usd,
        max(iff(metric = 'contribution_margin_usd', value, null))       as contribution_margin_usd,
        max(iff(metric = 'revenue_orders', value, null))                as revenue_orders,
        max(iff(metric = 'new_customers', value, null))                 as new_customers,
        max(iff(metric = 'new_subscriptions', value, null))             as new_subscriptions,
        max(iff(metric = 'canceled_subscriptions', value, null))        as canceled_subscriptions,
        max(iff(metric = 'ad_spend_usd', value, null))                  as ad_spend_usd
    from {{ ref('exec_kpi_anomalies') }}
    group by brand, date_day

),

expected as (

    select *
    from {{ ref('exec_daily_brand_metrics') }}
    where date_day > dateadd(day, -{{ var('anomaly_window_days') }}, {{ as_of_date() }})

)

select
    coalesce(e.brand, r.brand)          as brand,
    coalesce(e.date_day, r.date_day)    as date_day,
    r.metrics
from expected as e
full outer join reported as r
    on e.brand = r.brand and e.date_day = r.date_day
where e.brand is null
   or r.brand is null
   or r.metrics <> 7
   or abs(r.revenue_usd - e.revenue_usd) > 0.005
   or abs(r.contribution_margin_usd - e.contribution_margin_usd) > 0.005
   or r.revenue_orders <> e.revenue_orders
   or r.new_customers <> e.new_customers
   or r.new_subscriptions <> e.new_subscriptions
   or r.canceled_subscriptions <> -e.canceled_subscriptions
   or abs(r.ad_spend_usd - e.ad_spend_usd) > 0.005
