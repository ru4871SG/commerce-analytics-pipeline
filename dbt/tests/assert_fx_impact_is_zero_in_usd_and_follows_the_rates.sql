{{ config(severity='warn') }}

with from_orders as (

    select
        brand,
        date_trunc(month, order_date)::date                 as month_start,
        sum(gross_sales_usd_actual - gross_sales_usd)       as fx_effect_usd
    from {{ ref('fct_orders') }}
    where is_revenue_order
      and order_date <= {{ as_of_date() }}
    group by brand, date_trunc(month, order_date)::date

),

from_refunds as (

    select
        brand,
        date_trunc(month, issued_date)::date                as month_start,
        sum(refund_amount_usd_actual - refund_amount_usd)   as fx_effect_usd
    from {{ ref('fct_refunds') }}
    where reduces_revenue
      and issued_date <= {{ as_of_date() }}
    group by brand, date_trunc(month, issued_date)::date

)

select
    f.brand,
    f.month_start,
    b.currency_code,
    f.revenue_usd_constant,
    f.revenue_usd_actual,
    f.fx_effect_usd,
    coalesce(o.fx_effect_usd, 0) - coalesce(r.fx_effect_usd, 0)     as fx_effect_from_the_rates
from {{ ref('exec_fx_impact') }} as f
inner join {{ ref('dim_brands') }} as b
    on f.brand = b.brand
left join from_orders as o
    on f.brand = o.brand and f.month_start = o.month_start
left join from_refunds as r
    on f.brand = r.brand and f.month_start = r.month_start
where (b.currency_code = 'USD' and f.fx_effect_usd <> 0)
   or abs(f.fx_effect_usd - (f.revenue_usd_actual - f.revenue_usd_constant)) > 0.01
   or abs(f.fx_effect_usd - (coalesce(o.fx_effect_usd, 0) - coalesce(r.fx_effect_usd, 0))) > 0.01
