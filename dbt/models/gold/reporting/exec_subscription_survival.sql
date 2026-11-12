{#
    Share of subscriptions still active N days after they started, for N = 0..curve_max_day,
    per brand. A subscription is a customer's first order when that order is a subscription.
    Feeds the "subscribers left" curve and the cancelled-within-30 / 60 / 90-days cards
    (cancelled = 1 - share). Only subscriptions at least N days old at the end of the as-of date
    count at day N: a younger one has not yet had N days to cancel in, and counting it would read
    as low churn. So the base shrinks as N grows, and the curve can tick up where it does. A day is
    left empty until at least one subscription is N days old.
#}
with subscriptions as (

    select
        brand,
        -- The first day offset it no longer counts as active on; null when never cancelled. One
        -- that ended before it started no longer counts from day 0.
        iff(
            first_order_subscription_canceled_at is null,
            null,
            greatest(ceil(datediff(second, first_order_at, first_order_subscription_canceled_at) / 86400), 0)
        )                                                       as ended_from_day,
        -- The oldest day offset it has reached by the end of the as-of date
        floor(
            datediff(second, first_order_at, {{ business_day_start('dateadd(day, 1, ' ~ as_of_date() ~ ')') }}) / 86400
        )                                                       as observed_days
    from {{ ref('dim_customers') }}
    where first_order_is_subscription

),

days as (

    {{ number_spine(var('curve_max_day')) }}

),

curve as (

    select
        s.brand,
        d.n                                                     as day_offset,
        count_if(s.observed_days >= d.n)::int                   as subscriptions,
        count_if(s.observed_days >= d.n
                 and (s.ended_from_day is null or s.ended_from_day > d.n))::int
                                                                as active_subscriptions
    from subscriptions as s
    cross join days as d
    group by s.brand, d.n

)

-- Each column is cast to its contract type (_reporting.yml)
select
    curve.brand::varchar                                                        as brand,
    brands.brand_name::varchar                                                  as brand_name,
    curve.day_offset::int                                                       as day_offset,
    curve.subscriptions::int                                                    as subscriptions,
    curve.active_subscriptions::int                                             as active_subscriptions,
    (curve.active_subscriptions / nullif(curve.subscriptions, 0))::float        as subscribers_left_rate,
    (1 - curve.active_subscriptions / nullif(curve.subscriptions, 0))::float    as canceled_rate
from curve
inner join {{ ref('brands') }} as brands
    on curve.brand = brands.brand
