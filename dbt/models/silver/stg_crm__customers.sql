-- One row per customer. The loader never copies email, so this view has no PII.
select
    lower(_brand)                  as brand,
    id                             as customer_id,
    country_code,
    coalesce(subscribed_to_newsletter, false)
                                   as is_subscribed_to_newsletter,
    created_at                     as signed_up_at,
    updated_at
from {{ source('crm', 'customers') }}
