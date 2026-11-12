select
    coalesce(o.order_key, s.subscription_key)       as order_key,
    o.order_date,
    s.started_on
from (
    select order_key, order_date
    from {{ ref('fct_orders') }}
    where is_paid_subscription
) as o
full outer join {{ ref('dim_subscriptions') }} as s
    on o.order_key = s.subscription_key
where o.order_key is null
   or s.subscription_key is null
   or o.order_date <> s.started_on
