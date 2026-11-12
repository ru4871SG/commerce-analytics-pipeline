with reported as (

    select
        brand,
        month_start,
        sum(units)                      as units,
        sum(line_revenue_usd)           as line_revenue_usd,
        sum(line_cost_usd)              as line_cost_usd,
        sum(line_margin_usd)            as line_margin_usd,
        sum(units_in_refunded_orders)   as units_in_refunded_orders
    from {{ ref('exec_product_performance') }}
    group by brand, month_start

),

refunds_per_order as (

    select order_key, count(*) as refunds
    from {{ ref('fct_refunds') }}
    where reduces_revenue
    group by order_key

),

revenue_orders as (

    select
        o.order_key,
        o.brand,
        date_trunc(month, o.order_date)::date   as month_start,
        o.items_total_cents,
        o.cost_of_order_cents,
        coalesce(r.refunds, 0)                  as refunds
    from {{ ref('fct_orders') }} as o
    left join refunds_per_order as r
        on o.order_key = r.order_key
    where o.is_revenue_order
      and o.order_date <= {{ as_of_date() }}

),

from_items as (

    select
        o.brand,
        o.month_start,
        sum(i.quantity)                         as units,
        sum(i.item_total_cents)                 as item_total_cents,
        sum(i.item_cost_cents)                  as item_cost_cents,
        sum(iff(o.refunds > 0, i.quantity, 0))  as units_in_refunded_orders
    from {{ ref('fct_order_items') }} as i
    inner join revenue_orders as o
        on i.order_key = o.order_key
    group by o.brand, o.month_start

),

from_orders as (

    select
        brand,
        month_start,
        sum(items_total_cents)                  as items_total_cents,
        sum(cost_of_order_cents)                as cost_of_order_cents
    from revenue_orders
    group by brand, month_start

),

brand_months as (

    select distinct
        brand,
        date_trunc(month, date_day)::date       as month_start
    from {{ ref('exec_daily_brand_metrics') }}

),

expected as (

    select
        m.brand,
        m.month_start,
        coalesce(i.units, 0)                                            as units,
        coalesce(i.units_in_refunded_orders, 0)                         as units_in_refunded_orders,
        iff(b.prices_include_vat, coalesce(i.item_total_cents, 0) / (1 + b.vat_rate), coalesce(i.item_total_cents, 0))
            / 100 * b.usd_rate                                          as line_revenue_usd,
        coalesce(i.item_cost_cents, 0) / 100 * b.usd_rate               as line_cost_usd,
        iff(b.prices_include_vat, coalesce(o.items_total_cents, 0) / (1 + b.vat_rate), coalesce(o.items_total_cents, 0))
            / 100 * b.usd_rate                                          as orders_items_total_usd,
        coalesce(o.cost_of_order_cents, 0) / 100 * b.usd_rate           as orders_cost_usd
    from brand_months as m
    inner join {{ ref('dim_brands') }} as b
        on m.brand = b.brand
    left join from_items as i
        on m.brand = i.brand and m.month_start = i.month_start
    left join from_orders as o
        on m.brand = o.brand and m.month_start = o.month_start

)

select
    coalesce(e.brand, r.brand)                  as brand,
    coalesce(e.month_start, r.month_start)      as month_start,
    e.units                                     as expected_units,
    r.units,
    e.line_revenue_usd                          as expected_line_revenue_usd,
    r.line_revenue_usd,
    e.line_cost_usd                             as expected_line_cost_usd,
    r.line_cost_usd,
    r.line_margin_usd,
    e.units_in_refunded_orders                  as expected_units_in_refunded_orders,
    r.units_in_refunded_orders,
    e.orders_items_total_usd,
    e.orders_cost_usd
from expected as e
full outer join reported as r
    on e.brand = r.brand and e.month_start = r.month_start
where e.brand is null
   or r.brand is null
   or r.units <> e.units
   or r.units_in_refunded_orders <> e.units_in_refunded_orders
   or abs(r.line_revenue_usd - e.line_revenue_usd) > 0.01
   or abs(r.line_cost_usd - e.line_cost_usd) > 0.01
   or abs(r.line_margin_usd - (e.line_revenue_usd - e.line_cost_usd)) > 0.01
   or abs(r.line_revenue_usd - e.orders_items_total_usd) > 0.01
   or abs(r.line_cost_usd - e.orders_cost_usd) > 0.01
