{{ config(materialized='table') }}

{#
    One row per brand and product variant. The grain is the variant, because order lines point
    at a variant; the product attributes and its fulfilment costs are denormalised onto it.
#}
select
    {{ dbt_utils.generate_surrogate_key(['v.brand', 'v.variant_id']) }} as product_variant_key,
    {{ dbt_utils.generate_surrogate_key(['v.brand']) }}                as brand_key,
    v.brand,
    v.variant_id,
    v.variant_code,
    v.purchase_type,
    v.list_price_cents,
    p.product_id,
    p.product_code,
    p.product_name,
    p.sku,
    p.mint_soft_sku,
    p.product_cost_sku,
    pc.shipping_cost_cents,
    pc.first_unit_cost_cents,
    pc.additional_unit_cost_cents
from {{ ref('stg_crm__product_variants') }} as v
inner join {{ ref('stg_crm__products') }} as p
    on v.brand = p.brand and v.product_id = p.product_id
left join {{ ref('stg_crm__product_costs') }} as pc
    on p.brand = pc.brand and p.product_cost_sku = pc.sku
