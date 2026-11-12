{#
    How often a new customer takes the upsell. One row per brand and month: the customers' first
    orders placed in that month, and how many of them have an upsell line (a variant whose
    purchase type is Upsell), so the attach rate is
    SUM(first_orders_with_upsell) / SUM(first_orders). A first order is the customer's earliest
    qualifying order (dim_customers.first_order_key), counted when it is a revenue order and not
    a renewal. Every month has a row, with zeros when no customer started in it.
#}
with brand_months as (

    {{ brand_months() }}

),

first_orders as (

    select
        o.brand,
        o.order_key,
        date_trunc(month, o.order_date)::date               as month_start
    from {{ ref('dim_customers') }} as c
    inner join {{ ref('fct_orders') }} as o
        on c.first_order_key = o.order_key
    where o.is_revenue_order
      and o.created_from_order_id is null
      and o.order_date <= {{ as_of_date() }}

),

orders_with_upsell as (

    select distinct i.order_key
    from {{ ref('fct_order_items') }} as i
    inner join {{ ref('dim_products') }} as p
        on i.product_variant_key = p.product_variant_key
    where p.purchase_type = 'Upsell'

),

by_month as (

    select
        f.brand,
        f.month_start,
        count(*)                                            as first_orders,
        count(u.order_key)                                  as first_orders_with_upsell
    from first_orders as f
    left join orders_with_upsell as u
        on f.order_key = u.order_key
    group by f.brand, f.month_start

)

-- Each column is cast to its contract type (_reporting.yml)
select
    m.brand::varchar                                        as brand,
    m.brand_name::varchar                                   as brand_name,
    m.month_start::date                                     as month_start,
    coalesce(f.first_orders, 0)::int                        as first_orders,
    coalesce(f.first_orders_with_upsell, 0)::int            as first_orders_with_upsell
from brand_months as m
left join by_month as f
    on m.brand = f.brand and m.month_start = f.month_start
