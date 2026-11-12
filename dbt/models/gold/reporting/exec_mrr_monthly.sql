{#
    Monthly recurring revenue (MRR) as a roll-forward. One row per brand and month: the balance
    the month opened with, what new and reactivated subscriptions added, what cancelled ones took
    away (churned, positive here), and the balance it closed with, in USD and in subscriptions:
    opening + new + reactivated - churned = closing, and a month opens where the month before
    closed. Revenue churn is SUM(churned_mrr_usd) / SUM(opening_mrr_usd). A balance is a
    snapshot: it adds up across brands, not across months. The month that holds the as-of date
    closes on that date, and is_complete_month is false for it until its last day. The paused part
    of a closing balance is the subscriptions the source flags as paused today: the flag has no
    date, and a paused subscription keeps renewing. MRR is dim_subscriptions.mrr_usd, in USD at
    constant-currency rates.
#}
{%- set so_far = 'over (partition by m.brand order by m.month_start rows between unbounded preceding and current row)' %}
{%- set before = 'over (partition by m.brand order by m.month_start rows between unbounded preceding and 1 preceding)' %}
with brand_months as (

    {{ brand_months() }}

),

movements as (

    {{ subscription_movements() }}

),

by_month as (

    -- Every sum of MRR, here and in the balances, is cast back to the six decimals it has on
    -- Snowflake. On the emulator a sum is a float, whose last bits depend on the order of the
    -- rows: without the cast a balance that returns to zero could be a hair below it
    select
        brand,
        month_start,
        sum(iff(movement_type = 'new', mrr_usd, 0))::number(38, 6)          as new_mrr_usd,
        sum(iff(movement_type = 'reactivated', mrr_usd, 0))::number(38, 6)  as reactivated_mrr_usd,
        (-sum(iff(movement_type = 'churned', mrr_usd, 0)))::number(38, 6)   as churned_mrr_usd,
        sum(mrr_usd)::number(38, 6)                                         as net_mrr_usd,
        sum(iff(is_paused, mrr_usd, 0))::number(38, 6)                      as net_paused_mrr_usd,
        sum(iff(movement_type = 'new', subscriptions, 0))                   as new_subscriptions,
        sum(iff(movement_type = 'reactivated', subscriptions, 0))           as reactivated_subscriptions,
        -sum(iff(movement_type = 'churned', subscriptions, 0))              as churned_subscriptions,
        sum(subscriptions)                                                  as net_subscriptions,
        sum(iff(is_paused, subscriptions, 0))                               as net_paused_subscriptions
    from movements
    group by brand, month_start

),

balances as (

    -- A balance is every movement so far: the closing one with the month's own, the opening one
    -- without them
    select
        m.brand,
        m.brand_name,
        m.month_start,
        coalesce(b.new_mrr_usd, 0)                                  as new_mrr_usd,
        coalesce(b.reactivated_mrr_usd, 0)                          as reactivated_mrr_usd,
        coalesce(b.churned_mrr_usd, 0)                              as churned_mrr_usd,
        coalesce(b.new_subscriptions, 0)                            as new_subscriptions,
        coalesce(b.reactivated_subscriptions, 0)                    as reactivated_subscriptions,
        coalesce(b.churned_subscriptions, 0)                        as churned_subscriptions,
        (sum(coalesce(b.net_mrr_usd, 0)) {{ before }})::number(38, 6)
                                                                    as opening_mrr_usd,
        (sum(coalesce(b.net_mrr_usd, 0)) {{ so_far }})::number(38, 6)
                                                                    as closing_mrr_usd,
        (sum(coalesce(b.net_paused_mrr_usd, 0)) {{ so_far }})::number(38, 6)
                                                                    as closing_paused_mrr_usd,
        sum(coalesce(b.net_subscriptions, 0)) {{ before }}          as opening_subscriptions,
        sum(coalesce(b.net_subscriptions, 0)) {{ so_far }}          as closing_subscriptions,
        sum(coalesce(b.net_paused_subscriptions, 0)) {{ so_far }}   as closing_paused_subscriptions
    from brand_months as m
    left join by_month as b
        on m.brand = b.brand and m.month_start = b.month_start

)

-- Each column is cast to its contract type (_reporting.yml)
select
    brand::varchar                                                  as brand,
    brand_name::varchar                                             as brand_name,
    month_start::date                                               as month_start,
    -- The first month has no month before it: it opens at zero
    coalesce(opening_mrr_usd, 0)::float                             as opening_mrr_usd,
    new_mrr_usd::float                                              as new_mrr_usd,
    reactivated_mrr_usd::float                                      as reactivated_mrr_usd,
    churned_mrr_usd::float                                          as churned_mrr_usd,
    closing_mrr_usd::float                                          as closing_mrr_usd,
    closing_paused_mrr_usd::float                                   as closing_paused_mrr_usd,
    coalesce(opening_subscriptions, 0)::int                         as opening_subscriptions,
    new_subscriptions::int                                          as new_subscriptions,
    reactivated_subscriptions::int                                  as reactivated_subscriptions,
    churned_subscriptions::int                                      as churned_subscriptions,
    closing_subscriptions::int                                      as closing_subscriptions,
    closing_paused_subscriptions::int                               as closing_paused_subscriptions,
    -- Over once its last day is no later than the as-of date
    (dateadd(day, -1, dateadd(month, 1, month_start)) <= {{ as_of_date() }})::boolean
                                                                    as is_complete_month
from balances
