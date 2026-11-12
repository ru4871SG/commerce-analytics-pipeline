{#
    One row per brand, day and channel of the storefronts' traffic: sessions, the sessions that
    reached each step of the funnel, purchases, and the channel's ad spend that day. Every column
    adds up across days, channels and brands, so a chart needs only SUM and a time grain. Spend is
    in USD at constant-currency rates; organic channels have none.
#}
with sessions as (

    select
        brand,
        business_date                       as date_day,
        channel,
        sum(sessions)                       as sessions,
        sum(engaged_sessions)               as engaged_sessions,
        sum(view_item_sessions)             as view_item_sessions,
        sum(add_to_cart_sessions)           as add_to_cart_sessions,
        sum(checkout_sessions)              as checkout_sessions,
        sum(purchase_sessions)              as purchase_sessions,
        sum(purchases)                      as purchases
    from {{ ref('stg_web__daily_funnel') }}
    group by brand, business_date, channel

),

spend as (

    select brand, spend_date as date_day, utm_source as channel, sum(spend_usd) as ad_spend_usd
    from {{ ref('fct_marketing_spend') }}
    group by brand, spend_date, utm_source

)

-- Each column is cast to its contract type (_reporting.yml)
select
    s.brand::varchar                        as brand,
    brands.brand_name::varchar              as brand_name,
    s.date_day::date                        as date_day,
    s.channel::varchar                      as channel,
    s.sessions::int                         as sessions,
    s.engaged_sessions::int                 as engaged_sessions,
    s.view_item_sessions::int               as view_item_sessions,
    s.add_to_cart_sessions::int             as add_to_cart_sessions,
    s.checkout_sessions::int                as checkout_sessions,
    s.purchase_sessions::int                as purchase_sessions,
    s.purchases::int                        as purchases,
    coalesce(sp.ad_spend_usd, 0)::float     as ad_spend_usd
from sessions as s
inner join {{ ref('brands') }} as brands
    on s.brand = brands.brand
left join spend as sp
    on s.brand = sp.brand and s.date_day = sp.date_day and s.channel = sp.channel
