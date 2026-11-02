-- The daily metrics must add up to the revenue orders and the refunds issued up to the as-of date,
-- per brand, for revenue and contribution margin. Returns the brands where either differs by more
-- than a cent.
with from_orders as (

    select
        brand,
        sum(gross_sales_usd) as gross_sales,
        sum(margin_before_refunds_usd) as margin_before_refunds
    from {{ ref('fct_orders') }}
    where is_revenue_order
      and order_date <= {{ as_of_date() }}
    group by brand

),

from_refunds as (

    select brand, sum(refund_amount_usd) as refunds
    from {{ ref('fct_refunds') }}
    where reduces_revenue
      and issued_date <= {{ as_of_date() }}
    group by brand

),

from_daily as (

    select
        brand,
        sum(revenue_usd) as revenue,
        sum(contribution_margin_usd) as contribution_margin
    from {{ ref('exec_daily_brand_metrics') }}
    group by brand

)

select
    o.brand,
    o.gross_sales,
    r.refunds,
    d.revenue as daily_revenue,
    o.margin_before_refunds,
    d.contribution_margin as daily_contribution_margin
from from_orders as o
left join from_refunds as r
    on o.brand = r.brand
full outer join from_daily as d
    on o.brand = d.brand
where abs(coalesce(o.gross_sales, 0) - coalesce(r.refunds, 0) - coalesce(d.revenue, 0)) > 0.01
   or abs(coalesce(o.margin_before_refunds, 0) - coalesce(r.refunds, 0) - coalesce(d.contribution_margin, 0)) > 0.01
