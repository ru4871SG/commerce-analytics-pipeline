-- Fulfilment cost per product cost SKU, in cents. One row per brand and SKU.
select
    lower(_brand)        as brand,
    sku,
    shipping_amount      as shipping_cost_cents,
    first_amount         as first_unit_cost_cents,
    addition_amount      as additional_unit_cost_cents
from {{ source('crm', 'product_costs') }}
qualify row_number() over (partition by _brand, sku order by id) = 1
