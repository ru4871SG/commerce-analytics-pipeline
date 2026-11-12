with weekly as (

    select brand, snapshot_date, active_subscriptions
    from {{ ref('exec_active_subscriptions_weekly') }}
    qualify row_number() over (partition by brand order by week_start desc) = 1

),

active as (

    select
        brand,
        count(iff(is_active, 1, null))              as active_subscriptions,
        sum(iff(is_active, mrr_usd, 0))             as active_mrr_usd
    from {{ ref('dim_subscriptions') }}
    group by brand

),

latest_month as (

    select brand, month_start, closing_subscriptions, closing_mrr_usd
    from {{ ref('exec_mrr_monthly') }}
    qualify row_number() over (partition by brand order by month_start desc) = 1

)

select
    m.brand,
    m.month_start,
    w.snapshot_date,
    w.active_subscriptions                          as weekly_active_subscriptions,
    a.active_subscriptions                          as dim_active_subscriptions,
    m.closing_subscriptions,
    a.active_mrr_usd,
    m.closing_mrr_usd
from latest_month as m
left join weekly as w
    on m.brand = w.brand
left join active as a
    on m.brand = a.brand
where m.closing_subscriptions <> coalesce(w.active_subscriptions, 0)
   or m.closing_subscriptions <> coalesce(a.active_subscriptions, 0)
   or abs(m.closing_mrr_usd - coalesce(a.active_mrr_usd, 0)) > 0.005
   or w.snapshot_date <> {{ as_of_date() }}
