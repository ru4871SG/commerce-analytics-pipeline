with daily as (

    select
        brand,
        date_trunc(month, date_day)::date               as month_start,
        sum(revenue_usd)                                as revenue_usd
    from {{ ref('exec_daily_brand_metrics') }}
    group by brand, date_trunc(month, date_day)::date

)

select
    coalesce(d.brand, f.brand)                          as brand,
    coalesce(d.month_start, f.month_start)              as month_start,
    d.revenue_usd,
    f.revenue_usd_constant
from daily as d
full outer join {{ ref('exec_fx_impact') }} as f
    on d.brand = f.brand and d.month_start = f.month_start
where d.brand is null
   or f.brand is null
   or abs(d.revenue_usd - f.revenue_usd_constant) > 0.01
