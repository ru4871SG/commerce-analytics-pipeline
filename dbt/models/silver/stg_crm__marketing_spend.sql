-- One row per brand, day, channel, medium and campaign of ad spend. The channel is the
-- utm_source a paid order carries, so spend joins to the customers it brought.
select
    lower(_brand)        as brand,
    id                   as marketing_spend_id,
    spend_date,
    lower(utm_source)    as utm_source,
    lower(utm_medium)    as utm_medium,
    lower(utm_campaign)  as utm_campaign,
    amount               as spend_cents,
    impressions,
    clicks,
    currency_code
from {{ source('crm', 'marketing_spend') }}
