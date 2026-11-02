-- One row per product variant. purchase_type is encoded in the variant code, as in the source
-- application: 'vip' -> Subscribe, '-u' -> Upsell, everything else -> One-Time Purchase.
select
    lower(_brand)        as brand,
    id                   as variant_id,
    product_id,
    code                 as variant_code,
    price                as list_price_cents,
    case
        when contains(code, 'vip') then 'Subscribe'
        when contains(code, '-u') then 'Upsell'
        else 'One-Time Purchase'
    end                  as purchase_type,
    position,
    on_hand,
    coalesce(tracked, false) as is_tracked,
    coalesce(shipping_required, false) as requires_shipping
from {{ source('crm', 'product_variants') }}
