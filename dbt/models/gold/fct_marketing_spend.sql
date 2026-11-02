{{ config(materialized='table') }}

{#
    One row per brand, day, channel, medium and campaign of ad spend. spend is in the brand's
    currency; spend_usd converts it at the constant-currency rate, as revenue is, so spend and
    revenue compare across brands. Ratios (CPC, CPM, CAC) are not stored because they do not add
    up: the reporting models compute them from the sums.
#}
select
    {{ dbt_utils.generate_surrogate_key(['s.brand', 's.marketing_spend_id']) }} as marketing_spend_key,
    b.brand_key,
    s.brand,
    s.marketing_spend_id,
    s.spend_date,
    s.utm_source,
    s.utm_medium,
    s.utm_campaign,
    s.spend_cents,
    round(s.spend_cents / 100, 2)::number(18, 2)                      as spend,
    round(s.spend_cents / 100 * b.usd_rate, 2)::number(18, 2)         as spend_usd,
    s.impressions,
    s.clicks
from {{ ref('stg_crm__marketing_spend') }} as s
inner join {{ ref('dim_brands') }} as b
    on s.brand = b.brand
