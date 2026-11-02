-- The line costs in fct_order_items must add up to cost_of_order_cents in fct_orders.
-- Returns the orders where they differ.
with from_items as (

    select brand, order_id, sum(item_cost_cents) as item_costs
    from {{ ref('fct_order_items') }}
    group by brand, order_id

),

from_orders as (

    select brand, order_id, cost_of_order_cents
    from {{ ref('fct_orders') }}

)

select
    o.brand,
    o.order_id,
    o.cost_of_order_cents,
    i.item_costs
from from_orders as o
left join from_items as i
    on o.brand = i.brand and o.order_id = i.order_id
where abs(o.cost_of_order_cents - coalesce(i.item_costs, 0)) > 0
