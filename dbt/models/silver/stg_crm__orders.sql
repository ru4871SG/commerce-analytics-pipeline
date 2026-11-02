-- A table, not a view: three models and several tests read it, and a view would convert every
-- order's timestamps to New York days again for each.
{{ config(materialized='table') }}

-- One row per order (carts included), all brands. Flags are booleans; amounts stay in cents.
-- Timestamps stay UTC, as the source stores them; the dates are New York days (business_date).
-- Calculated columns from the old pipeline (acquisition type, week strings, pause flags) are
-- deliberately not here: dbt derives them in the gold layer.
select
    lower(_brand)                                    as brand,
    id                                               as order_id,
    number                                           as order_number,
    customer_id,
    created_from_order_id,
    state,
    payment_state,
    shipping_state,
    checkout_completed_at,
    items_total                                      as items_total_cents,
    adjustments_total                                as adjustments_total_cents,
    total                                            as total_cents,
    currency_code,
    {{ is_paid('payment_state') }}                   as is_paid,
    -- The orders that make someone a customer and count as customer activity: paid, fulfilled
    -- or new, with a positive total.
    {{ is_paid('payment_state') }}
        and state in ('fulfilled', 'new')
        and total > 0                                as is_qualifying_order,
    created_at,
    {{ business_date('created_at') }}                as order_date,
    updated_at,
    coalesce(is_subscription, false)                 as is_subscription,
    coalesce(is_subscription_canceled, false)        as is_subscription_canceled,
    subscription_canceled_at,
    {{ business_date('subscription_canceled_at') }}
                                                     as cancel_date,
    coalesce(subscription_paused, false)             as is_subscription_paused,
    coalesce(is_created_by_admin, false)             as is_created_by_admin,
    coalesce(is_sales_funnel, false)                 as is_sales_funnel,
    acquisition_domain,
    utm_source,
    utm_medium,
    utm_campaign,
    utm_content,
    utm_term,
    utm_id,
    _loaded_at
from {{ source('crm', 'orders') }}
