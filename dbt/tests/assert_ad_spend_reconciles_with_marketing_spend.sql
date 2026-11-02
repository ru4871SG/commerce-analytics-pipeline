-- Ad spend must add up wherever the dashboard shows it, per brand: the daily metrics against
-- the spend fact up to the as-of date, and the CAC table against the spend fact over its
-- acquisition window. Returns the brands and tables where they differ by more than a cent.
with daily as (

    select brand, sum(ad_spend_usd) as reported
    from {{ ref('exec_daily_brand_metrics') }}
    group by brand

),

daily_expected as (

    select brand, sum(spend_usd) as expected
    from {{ ref('fct_marketing_spend') }}
    where spend_date between (select min(order_date) from {{ ref('fct_orders') }}) and {{ as_of_date() }}
    group by brand

),

cac as (

    select brand, sum(spend_usd) as reported, min(acquisition_window_start) as window_start,
           min(acquisition_window_end) as window_end
    from {{ ref('exec_channel_cac_ltv') }}
    group by brand

),

cac_expected as (

    select s.brand, sum(s.spend_usd) as expected
    from {{ ref('fct_marketing_spend') }} as s
    inner join cac as c
        on s.brand = c.brand and s.spend_date between c.window_start and c.window_end
    group by s.brand

)

select 'exec_daily_brand_metrics' as reported_in, e.brand, d.reported, e.expected
from daily_expected as e
left join daily as d
    on e.brand = d.brand
where abs(coalesce(d.reported, 0) - e.expected) > 0.01

union all

select 'exec_channel_cac_ltv', c.brand, c.reported, coalesce(e.expected, 0)
from cac as c
left join cac_expected as e
    on c.brand = e.brand
where abs(c.reported - coalesce(e.expected, 0)) > 0.01
