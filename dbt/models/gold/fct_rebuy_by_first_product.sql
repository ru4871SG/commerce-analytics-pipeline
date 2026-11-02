{{ config(materialized='table') }}

{#
    For each brand, first product and purchase type: how many customers ordered again within N
    days of their first order, for N = 0..curve_max_day. A rebuy is a later order on a later
    calendar day, with the order filters the report used (not cancelled, a customer, an order
    number). Replaces the daily_rebuy_percentage tables.
#}
with customers as (

    select brand, customer_id, first_order_date, first_product_name, first_purchase_type
    from {{ ref('dim_customers') }}
    where first_product_name is not null
      and first_purchase_type is not null
      and first_order_date is not null

),

first_rebuy as (

    select
        c.brand,
        c.customer_id,
        min(datediff(day, c.first_order_date, o.order_date)) as first_rebuy_day
    from customers as c
    inner join {{ ref('fct_orders') }} as o
        on c.brand = o.brand
        and c.customer_id = o.customer_id
        and o.order_date > c.first_order_date
        -- A missing payment state counts, as BLANK is not in the list in DAX (NULL would drop it)
        and coalesce(o.payment_state, '') not in ('cancelled', 'cart')
        and coalesce(o.order_number, '') <> ''
    group by c.brand, c.customer_id

),

groups as (

    select brand, first_product_name, first_purchase_type, count(*) as total_customers
    from customers
    group by brand, first_product_name, first_purchase_type

),

rebuys_by_day as (

    select c.brand, c.first_product_name, c.first_purchase_type, r.first_rebuy_day, count(*) as customers
    from customers as c
    inner join first_rebuy as r
        on c.brand = r.brand and c.customer_id = r.customer_id
    group by c.brand, c.first_product_name, c.first_purchase_type, r.first_rebuy_day

),

days as (

    {{ number_spine(var('curve_max_day')) }}

),

curve as (

    -- The customers whose first rebuy fell on each day, summed up to each day: one row per point
    -- of the curve, where joining every earlier day to each point grew with their product
    select
        g.brand,
        g.first_product_name,
        g.first_purchase_type,
        d.n                                                      as day_offset,
        g.total_customers,
        sum(coalesce(r.customers, 0)) over (
            partition by g.brand, g.first_product_name, g.first_purchase_type
            order by d.n
            rows between unbounded preceding and current row
        )                                                        as rebuyers
    from groups as g
    cross join days as d
    left join rebuys_by_day as r
        on g.brand = r.brand
        and g.first_product_name = r.first_product_name
        and g.first_purchase_type = r.first_purchase_type
        and r.first_rebuy_day = d.n

)

select
    brand,
    first_product_name,
    first_purchase_type,
    day_offset,
    rebuyers,
    total_customers,
    rebuyers / total_customers                                   as rebuy_rate
from curve
