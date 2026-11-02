-- One row per order line, all brands.
select
    lower(_brand)        as brand,
    id                   as order_item_id,
    order_id,
    variant_id,
    quantity,
    unit_price           as unit_price_cents,
    units_total          as units_total_cents,
    adjustments_total    as item_adjustments_cents,
    total                as item_total_cents,
    product_name,
    variant_name
from {{ source('crm', 'order_items') }}
