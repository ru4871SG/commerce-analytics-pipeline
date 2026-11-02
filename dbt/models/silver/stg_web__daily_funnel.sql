-- One row per brand, New York day, channel and device of the storefronts' sessions, from the
-- lakehouse's gold. A session counts on the day, channel and device of its first event; the
-- funnel columns count the sessions that reached each step.
select
    lower(brand)            as brand,
    business_date,
    lower(channel)          as channel,
    lower(device)           as device,
    sessions,
    engaged_sessions,
    view_item_sessions,
    add_to_cart_sessions,
    checkout_sessions,
    purchase_sessions,
    purchases,
    events,
    _batch_id               as batch_id,
    _engine                 as engine,
    _loaded_at
from {{ source('web', 'daily_funnel') }}
