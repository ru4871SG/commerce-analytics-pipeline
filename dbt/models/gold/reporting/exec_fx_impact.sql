{#
    What the exchange rate did to revenue. One row per brand and month: revenue at the constant
    (budget) rate, as every other table of the report has it, the same orders and refunds at
    the ECB's rate of their own day, and the difference between the two. Revenue is gross sales
    of revenue orders less the refunds issued in the month, as in exec_daily_brand_metrics, so
    revenue_usd_constant adds up to that table's revenue_usd. An order is converted at the rate
    of the day it was placed, a refund at the rate of the day it was issued. A brand that sells
    in USD has no difference. Every month has a row, with zeros when nothing was sold, so every
    column adds up and a chart is never empty. Source of the rates: ECB statistics.
#}
with brand_months as (

    {{ brand_months() }}

),

orders_by_month as (

    -- Each sum is cast back to the six decimals it has on Snowflake: on the emulator a sum is a
    -- float, and the difference of two floats that should be equal is not always zero
    select
        brand,
        date_trunc(month, order_date)::date                 as month_start,
        sum(gross_sales_usd)::number(38, 6)                 as gross_sales_usd,
        sum(gross_sales_usd_actual)::number(38, 6)          as gross_sales_usd_actual
    from {{ ref('fct_orders') }}
    where is_revenue_order
      and order_date <= {{ as_of_date() }}
    group by brand, date_trunc(month, order_date)::date

),

refunds_by_month as (

    select
        brand,
        date_trunc(month, issued_date)::date                as month_start,
        sum(refund_amount_usd)::number(38, 6)               as refunds_usd,
        sum(refund_amount_usd_actual)::number(38, 6)        as refunds_usd_actual
    from {{ ref('fct_refunds') }}
    where reduces_revenue
      and issued_date <= {{ as_of_date() }}
    group by brand, date_trunc(month, issued_date)::date

),

revenue as (

    select
        m.brand,
        m.brand_name,
        m.month_start,
        coalesce(o.gross_sales_usd, 0) - coalesce(rf.refunds_usd, 0)                    as revenue_usd_constant,
        coalesce(o.gross_sales_usd_actual, 0) - coalesce(rf.refunds_usd_actual, 0)      as revenue_usd_actual
    from brand_months as m
    left join orders_by_month as o
        on m.brand = o.brand and m.month_start = o.month_start
    left join refunds_by_month as rf
        on m.brand = rf.brand and m.month_start = rf.month_start

)

-- Each column is cast to its contract type (_reporting.yml)
select
    brand::varchar                                          as brand,
    brand_name::varchar                                     as brand_name,
    month_start::date                                       as month_start,
    revenue_usd_constant::float                             as revenue_usd_constant,
    revenue_usd_actual::float                               as revenue_usd_actual,
    (revenue_usd_actual - revenue_usd_constant)::float      as fx_effect_usd
from revenue
