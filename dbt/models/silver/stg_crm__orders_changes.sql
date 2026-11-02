-- Every version of an order the loader has copied (RAW.CRM.ORDERS_CHANGES), with the columns
-- that describe its state. One row per order and load in which it had changed.
select
    lower(_brand)                                    as brand,
    id                                               as order_id,
    state,
    payment_state,
    shipping_state,
    coalesce(is_subscription_canceled, false)        as is_subscription_canceled,
    subscription_canceled_at,
    coalesce(subscription_paused, false)             as is_subscription_paused,
    updated_at,
    _loaded_at                                       as loaded_at
from {{ source('crm', 'orders_changes') }}
