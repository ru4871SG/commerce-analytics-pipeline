with by_reason as (

    select
        brand,
        month_start,
        sum(refund_amount_usd) as refund_amount_usd
    from {{ ref('exec_refund_reasons') }}
    group by brand, month_start

),

daily as (

    select
        brand,
        date_trunc(month, date_day)::date as month_start,
        sum(refunds_usd) as refunds_usd
    from {{ ref('exec_daily_brand_metrics') }}
    group by brand, date_trunc(month, date_day)::date

)

select
    coalesce(d.brand, r.brand)                  as brand,
    coalesce(d.month_start, r.month_start)      as month_start,
    d.refunds_usd,
    r.refund_amount_usd
from daily as d
full outer join by_reason as r
    on d.brand = r.brand and d.month_start = r.month_start
where d.brand is null
   or r.brand is null
   or abs(d.refunds_usd - r.refund_amount_usd) > 0.01
