{#
    The storefront funnel per brand and channel over the 90 days that end on the as-of date: of
    the sessions, the share that reached each step, and what a session and a purchase cost the
    channel in ad spend. One row per brand and channel, so a chart uses MAX for one brand; across
    brands AVG averages the brands' rates, as the CAC and churn cards do.
#}
with window_days as (

    select dateadd(day, -89, {{ as_of_date() }})::date as window_start, {{ as_of_date() }} as window_end

),

totals as (

    select
        d.brand,
        d.channel,
        sum(d.sessions)                     as sessions,
        sum(d.engaged_sessions)             as engaged_sessions,
        sum(d.view_item_sessions)           as view_item_sessions,
        sum(d.add_to_cart_sessions)         as add_to_cart_sessions,
        sum(d.checkout_sessions)            as checkout_sessions,
        sum(d.purchase_sessions)            as purchase_sessions,
        sum(d.purchases)                    as purchases,
        sum(d.ad_spend_usd)                 as ad_spend_usd
    from {{ ref('exec_web_daily_sessions') }} as d
    cross join window_days as w
    where d.date_day between w.window_start and w.window_end
    group by d.brand, d.channel

)

-- Each column is cast to its contract type (_reporting.yml)
select
    t.brand::varchar                                                      as brand,
    brands.brand_name::varchar                                            as brand_name,
    t.channel::varchar                                                    as channel,
    w.window_start::date                                                  as window_start,
    w.window_end::date                                                    as window_end,
    t.sessions::int                                                       as sessions,
    t.purchases::int                                                      as purchases,
    t.ad_spend_usd::float                                                 as ad_spend_usd,
    round(t.engaged_sessions / t.sessions, 4)::number(18, 4)              as engaged_rate,
    round(t.view_item_sessions / t.sessions, 4)::number(18, 4)            as view_item_rate,
    round(t.add_to_cart_sessions / t.sessions, 4)::number(18, 4)          as add_to_cart_rate,
    round(t.checkout_sessions / t.sessions, 4)::number(18, 4)             as checkout_rate,
    round(t.purchase_sessions / t.sessions, 4)::number(18, 4)             as conversion_rate,
    iff(t.ad_spend_usd > 0, round(t.ad_spend_usd / t.sessions, 2), null)::number(18, 2)
                                                                          as cost_per_session_usd,
    iff(t.ad_spend_usd > 0, round(t.ad_spend_usd / nullif(t.purchases, 0), 2), null)::number(18, 2)
                                                                          as cost_per_purchase_usd
from totals as t
cross join window_days as w
inner join {{ ref('brands') }} as brands
    on t.brand = brands.brand
where t.sessions > 0
