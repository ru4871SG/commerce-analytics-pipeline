-- A table, not a view: dim_customers and fct_orders both read it, and a view would be
-- computed again for each.
{{ config(materialized='table') }}

{#
    The first item (lowest item id) of every order: its product name (corrected for known typos
    and offer names), quantity, variant and the warehouse SKU from the product catalogue.
#}
with first_items as (

    select *
    from {{ ref('stg_crm__order_items') }}
    qualify row_number() over (partition by brand, order_id order by order_item_id) = 1

)

select
    fi.brand,
    fi.order_id,
    fi.order_item_id                                           as first_order_item_id,
    fi.variant_id                                              as first_variant_id,
    v.variant_code                                             as first_variant_code,
    v.purchase_type                                            as first_purchase_type,
    coalesce(
        pn.product_name,
        nullif(trim(fi.product_name), ''),
        p.product_name
    )                                                          as first_item_name,
    fi.variant_name                                            as first_variant_name,
    fi.quantity                                                as first_item_quantity,
    p.mint_soft_sku                                            as first_item_warehouse_sku
from first_items as fi
left join {{ ref('stg_crm__product_variants') }} as v
    on fi.brand = v.brand and fi.variant_id = v.variant_id
left join {{ ref('stg_crm__products') }} as p
    on v.brand = p.brand and v.product_id = p.product_id
left join {{ ref('product_name_corrections') }} as pn
    on fi.product_name = pn.raw_product_name
