-- A table, not a view: fct_order_items and int_order_costs (so fct_orders) both read it,
-- and a view would be computed again for each.
{{ config(materialized='table') }}

{#
    Cost of each order line, in cents (ported from etl_costoforder_calculatedcolumns.py).
    The first item of an order carries the fixed costs: a COGS estimate on its share of the
    order value, shipping, first-item processing and the product's first-unit cost. Every
    further unit, on the first item or on later items, costs the additional processing plus the
    additional-unit cost. Items whose product has no product_cost_sku keep a NULL cost and are
    skipped when the order total is summed, as pandas' sum() skipped them.
#}
with items as (

    select
        oi.brand,
        oi.order_id,
        oi.order_item_id,
        oi.quantity,
        -- The item's own total plus the order's adjustments (shipping, discounts)
        oi.item_total_cents + o.total_cents - o.items_total_cents       as initial_order_value_cents,
        pc.shipping_cost_cents,
        pc.first_unit_cost_cents,
        pr.first_item_processing_cents,
        pr.additional_item_processing_cents + pc.additional_unit_cost_cents
                                                                        as additional_unit_total_cents,
        row_number() over (
            partition by oi.brand, oi.order_id
            order by oi.order_item_id
        )                                                               as item_position
    from {{ ref('stg_crm__order_items') }} as oi
    inner join {{ ref('stg_crm__orders') }} as o
        on oi.brand = o.brand and oi.order_id = o.order_id
    inner join {{ ref('stg_crm__product_variants') }} as v
        on oi.brand = v.brand and oi.variant_id = v.variant_id
    inner join {{ ref('stg_crm__products') }} as p
        on v.brand = p.brand and v.product_id = p.product_id
    left join {{ ref('stg_crm__product_costs') }} as pc
        on p.brand = pc.brand and p.product_cost_sku = pc.sku
    inner join {{ ref('stg_crm__processing_costs') }} as pr
        on oi.brand = pr.brand
    where o.state <> 'cart'

)

select
    brand,
    order_id,
    order_item_id,
    item_position,
    case
        when item_position = 1 then
            -- COGS estimate: 10% of the value plus the average of two payment-fee schedules
            trunc(
                initial_order_value_cents * 0.1
                + ((20 + initial_order_value_cents * 0.014) + (30 + initial_order_value_cents * 0.029)) / 2
            )
            + shipping_cost_cents
            + first_item_processing_cents
            + first_unit_cost_cents
            + (quantity - 1) * additional_unit_total_cents
        else quantity * additional_unit_total_cents
    end                                                             as item_cost_cents
from items
