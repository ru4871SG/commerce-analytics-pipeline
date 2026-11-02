-- Every storefront order (not a renewal, not one an agent created, not a cart) is a purchase in
-- the clickstream, in a session that started the same New York day. Returns the brands and days
-- where the two differ, over the days the clickstream covers. The demo's collector makes them
-- agree exactly; on real data, consent banners and ad blockers lose a few purchases, and the
-- test would allow a tolerance.
with purchases as (

    select brand, business_date, sum(purchases) as purchases
    from {{ ref('stg_web__daily_funnel') }}
    group by brand, business_date

),

orders as (

    select brand, order_date, count(*) as orders
    from {{ ref('stg_crm__orders') }}
    where state <> 'cart'
      and created_from_order_id is null
      and not is_created_by_admin
    group by brand, order_date

)

select p.brand, p.business_date, p.purchases, coalesce(o.orders, 0) as orders
from purchases as p
left join orders as o
    on p.brand = o.brand and p.business_date = o.order_date
where p.purchases <> coalesce(o.orders, 0)
