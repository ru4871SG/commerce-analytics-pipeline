with last_days as (

    select
        brand,
        date_trunc(month, date_day)::date                           as month_start,
        max(date_day)                                               as closes_on
    from {{ ref('exec_daily_brand_metrics') }}
    group by brand, date_trunc(month, date_day)::date

),

month_ends as (

    select
        m.*,
        d.closes_on,
        dateadd(day, -1, dateadd(month, 1, m.month_start))::date    as last_day_of_month
    from {{ ref('exec_mrr_monthly') }} as m
    left join last_days as d
        on m.brand = d.brand and m.month_start = d.month_start

),

running as (

    select
        e.brand,
        e.month_start,
        count(s.subscription_key)                                   as subscriptions,
        coalesce(sum(s.mrr_usd), 0)                                 as mrr_usd,
        count(iff(s.is_paused, 1, null))                            as paused_subscriptions,
        coalesce(sum(iff(s.is_paused, s.mrr_usd, 0)), 0)            as paused_mrr_usd
    from month_ends as e
    left join {{ ref('dim_subscriptions') }} as s
        on e.brand = s.brand
        and s.started_on <= e.closes_on
        and (s.ended_on is null or s.ended_on > e.closes_on)
    group by e.brand, e.month_start

)

select
    e.brand,
    e.month_start,
    e.closes_on,
    e.is_complete_month,
    e.closing_mrr_usd,
    r.mrr_usd                                                       as running_mrr_usd,
    e.closing_subscriptions,
    r.subscriptions                                                 as running_subscriptions,
    e.closing_paused_mrr_usd,
    r.paused_mrr_usd                                                as running_paused_mrr_usd,
    e.closing_paused_subscriptions,
    r.paused_subscriptions                                          as running_paused_subscriptions
from month_ends as e
inner join running as r
    on e.brand = r.brand and e.month_start = r.month_start
where e.closes_on is null
   or abs(e.closing_mrr_usd - r.mrr_usd) > 0.005
   or e.closing_subscriptions <> r.subscriptions
   or abs(e.closing_paused_mrr_usd - r.paused_mrr_usd) > 0.005
   or e.closing_paused_subscriptions <> r.paused_subscriptions
   or e.is_complete_month <> (e.closes_on = e.last_day_of_month)
