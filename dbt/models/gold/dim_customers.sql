{{ config(materialized='table') }}

{#
    One row per brand and customer, including customers who never ordered. first_order_* is the
    customer's earliest qualifying order (paid, fulfilled or new, positive total; see
    stg_crm__orders.is_qualifying_order); it is NULL when they have none. Test accounts are excluded.
#}
with customers as (

    select *
    from {{ ref('stg_crm__customers') }}
    where customer_id not in ({{ var('excluded_customer_ids') | join(', ') }})

),

first_orders as (

    select
        o.*,
        row_number() over (
            partition by o.brand, o.customer_id
            order by o.created_at, o.order_id
        ) as first_order_rank,
        -- Whether any of the customer's qualifying orders is a subscription, in the same pass
        max(iff(o.is_subscription, 1, 0)) over (partition by o.brand, o.customer_id) = 1
                                                                         as has_subscription_order
    from {{ ref('stg_crm__orders') }} as o
    where o.is_qualifying_order
      and o.customer_id is not null

),

first_order as (

    select * from first_orders where first_order_rank = 1

)

select
    {{ dbt_utils.generate_surrogate_key(['c.brand', 'c.customer_id']) }} as customer_key,
    {{ dbt_utils.generate_surrogate_key(['c.brand']) }}                   as brand_key,
    c.brand,
    c.customer_id,
    c.signed_up_at,
    c.country_code,
    c.is_subscribed_to_newsletter,
    case when fo.order_id is not null
        then {{ dbt_utils.generate_surrogate_key(['c.brand', 'fo.order_id']) }}
    end                                                                   as first_order_key,
    fo.order_id                                                           as first_order_id,
    fo.order_number                                                       as first_order_number,
    fo.created_at                                                         as first_order_at,
    fo.order_date                                                         as first_order_date,
    date_trunc(month, fo.order_date)::date                                as first_order_month,
    fi.first_item_name                                                     as first_product_name,
    fi.first_variant_code,
    fi.first_purchase_type,
    fo.is_subscription                                                    as first_order_is_subscription,
    fo.is_subscription_canceled                                           as first_order_is_subscription_canceled,
    fo.subscription_canceled_at                                           as first_order_subscription_canceled_at,
    fo.utm_source                                                         as attributed_utm_source,
    coalesce(fo.has_subscription_order, false)                            as has_subscription_order
from customers as c
left join first_order as fo
    on c.brand = fo.brand and c.customer_id = fo.customer_id
left join {{ ref('int_order_first_items') }} as fi
    on fo.brand = fi.brand and fo.order_id = fi.order_id
