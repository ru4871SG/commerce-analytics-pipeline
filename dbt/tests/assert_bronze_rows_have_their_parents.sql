-- Every row a foreign key points from must find the row it points at, within its brand. The
-- source enforces these keys; bronze cannot, and the gold facts inner-join through them, so an
-- order line whose order is missing would silently drop out of revenue. Returns the orphans.
select 'order_items.order_id' as foreign_key, i.brand, i.order_item_id as row_id
from {{ ref('stg_crm__order_items') }} as i
left join {{ ref('stg_crm__orders') }} as o
    on i.brand = o.brand and i.order_id = o.order_id
where o.order_id is null

union all

select 'order_items.variant_id', i.brand, i.order_item_id
from {{ ref('stg_crm__order_items') }} as i
left join {{ ref('stg_crm__product_variants') }} as v
    on i.brand = v.brand and i.variant_id = v.variant_id
where v.variant_id is null

union all

select 'orders.customer_id', o.brand, o.order_id
from {{ ref('stg_crm__orders') }} as o
left join {{ ref('stg_crm__customers') }} as c
    on o.brand = c.brand and o.customer_id = c.customer_id
where o.customer_id is not null and c.customer_id is null

union all

select 'orders.created_from_order_id', o.brand, o.order_id
from {{ ref('stg_crm__orders') }} as o
left join {{ ref('stg_crm__orders') }} as parent
    on o.brand = parent.brand and o.created_from_order_id = parent.order_id
where o.created_from_order_id is not null and parent.order_id is null

union all

select 'refund_payments.order_number', r.brand, r.refund_payment_id
from {{ ref('stg_crm__refund_payments') }} as r
left join {{ ref('stg_crm__orders') }} as o
    on r.brand = o.brand and r.order_number = o.order_number
where o.order_id is null
