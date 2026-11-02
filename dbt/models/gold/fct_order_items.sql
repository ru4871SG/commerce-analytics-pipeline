{{ config(materialized='table') }}

{#
    One row per order line of a non-cart order. Carries the line's cost so margin can be
    analysed at product level without recomputing the cost rule.
#}
select
    {{ dbt_utils.generate_surrogate_key(['oi.brand', 'oi.order_item_id']) }} as order_item_key,
    o.order_key,
    o.brand_key,
    p.product_variant_key,
    o.order_date,
    oi.brand,
    oi.order_id,
    oi.order_item_id,
    oi.variant_id,
    oi.quantity,
    oi.unit_price_cents,
    oi.units_total_cents,
    oi.item_adjustments_cents,
    oi.item_total_cents,
    c.item_position,
    coalesce(c.item_cost_cents, 0)::number(38, 0)                             as item_cost_cents,
    p.product_id,
    p.product_name,
    p.variant_code
from {{ ref('stg_crm__order_items') }} as oi
inner join {{ ref('fct_orders') }} as o
    on oi.brand = o.brand and oi.order_id = o.order_id
inner join {{ ref('dim_products') }} as p
    on oi.brand = p.brand and oi.variant_id = p.variant_id
left join {{ ref('int_order_item_costs') }} as c
    on oi.brand = c.brand and oi.order_item_id = c.order_item_id
