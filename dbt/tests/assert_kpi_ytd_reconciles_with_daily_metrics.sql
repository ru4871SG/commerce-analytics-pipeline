-- Revenue and contribution margin YTD on the number cards must equal the year's days in the
-- daily metrics, per brand.
with kpi as (

    select
        brand,
        as_of_date,
        revenue_ytd_usd as revenue_ytd,
        contribution_margin_ytd_usd as contribution_margin_ytd
    from {{ ref('exec_kpi_snapshot') }}

),

daily as (

    select
        d.brand,
        sum(d.revenue_usd) as revenue_ytd,
        sum(d.contribution_margin_usd) as contribution_margin_ytd
    from {{ ref('exec_daily_brand_metrics') }} as d
    inner join kpi as k
        on d.brand = k.brand
        and d.date_day between date_trunc(year, k.as_of_date) and k.as_of_date
    group by d.brand

)

select
    k.brand,
    k.revenue_ytd as card_ytd,
    d.revenue_ytd as daily_ytd,
    k.contribution_margin_ytd as card_margin_ytd,
    d.contribution_margin_ytd as daily_margin_ytd
from kpi as k
left join daily as d
    on k.brand = d.brand
where abs(k.revenue_ytd - coalesce(d.revenue_ytd, 0)) > 0.01
   or abs(k.contribution_margin_ytd - coalesce(d.contribution_margin_ytd, 0)) > 0.01
