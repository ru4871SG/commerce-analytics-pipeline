-- Product catalogue, one row per product and brand.
select
    lower(_brand)        as brand,
    id                   as product_id,
    code                 as product_code,
    name                 as product_name,
    enabled              as is_enabled,
    sku,
    mint_soft_sku,
    mint_soft_name,
    product_cost_sku
from {{ source('crm', 'products') }}
