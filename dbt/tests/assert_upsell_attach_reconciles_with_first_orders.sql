with upsell_lines as (

    select i.order_key, count(*) as upsell_lines
    from {{ ref('fct_order_items') }} as i
    inner join {{ ref('dim_products') }} as p
        on i.product_variant_key = p.product_variant_key
    where p.purchase_type = 'Upsell'
    group by i.order_key

),

from_customers as (

    select
        c.brand,
        c.first_order_month                             as month_start,
        count(*)                                        as first_orders,
        count(iff(u.upsell_lines > 0, 1, null))         as first_orders_with_upsell
    from {{ ref('dim_customers') }} as c
    inner join {{ ref('fct_orders') }} as o
        on c.first_order_key = o.order_key
    left join upsell_lines as u
        on c.first_order_key = u.order_key
    where o.is_revenue_order
      and o.renewal_of_order_key is null
      and c.first_order_date <= {{ as_of_date() }}
    group by c.brand, c.first_order_month

),

daily as (

    select
        brand,
        date_trunc(month, date_day)::date               as month_start,
        sum(new_customers)                              as new_customers
    from {{ ref('exec_daily_brand_metrics') }}
    group by brand, date_trunc(month, date_day)::date

)

select
    coalesce(d.brand, r.brand)                          as brand,
    coalesce(d.month_start, r.month_start)              as month_start,
    d.new_customers,
    coalesce(c.first_orders, 0)                         as expected_first_orders,
    r.first_orders,
    coalesce(c.first_orders_with_upsell, 0)             as expected_first_orders_with_upsell,
    r.first_orders_with_upsell
from daily as d
left join from_customers as c
    on d.brand = c.brand and d.month_start = c.month_start
full outer join {{ ref('exec_upsell_attach') }} as r
    on d.brand = r.brand and d.month_start = r.month_start
where d.brand is null
   or r.brand is null
   or r.first_orders <> coalesce(c.first_orders, 0)
   or r.first_orders_with_upsell <> coalesce(c.first_orders_with_upsell, 0)
   or r.first_orders > d.new_customers
