{#
    What moved monthly recurring revenue (MRR). One row per brand, month and movement type: the
    MRR and the subscriptions that started in the month (new, or reactivated when the customer
    came back after every earlier subscription had ended) and those that ended in it (churned,
    negative, so its bars point down and the sum of the three types is the month's net change).
    MRR is dim_subscriptions.mrr_usd: one unit of the subscription's line a month, VAT removed,
    in USD at constant-currency rates. Every type has a row in every month, with zeros when
    nothing moved, so every column adds up and a chart is never empty. The month that holds the
    as-of date runs to that date.
#}
with brand_months as (

    {{ brand_months() }}

),

movement_types as (

    select 'new' as movement_type
    union all
    select 'reactivated' as movement_type
    union all
    select 'churned' as movement_type

),

movements as (

    {{ subscription_movements() }}

),

by_type as (

    -- The sum is cast back to the six decimals it has on Snowflake: on the emulator a sum is a
    -- float, whose last bits depend on the order of the rows
    select
        brand,
        month_start,
        movement_type,
        sum(mrr_usd)::number(38, 6)                         as mrr_usd,
        sum(subscriptions)                                  as subscriptions
    from movements
    group by brand, month_start, movement_type

)

-- Each column is cast to its contract type (_reporting.yml)
select
    m.brand::varchar                                        as brand,
    m.brand_name::varchar                                   as brand_name,
    m.month_start::date                                     as month_start,
    t.movement_type::varchar                                as movement_type,
    coalesce(b.mrr_usd, 0)::float                           as mrr_usd,
    coalesce(b.subscriptions, 0)::int                       as subscriptions
from brand_months as m
cross join movement_types as t
left join by_type as b
    on m.brand = b.brand
    and m.month_start = b.month_start
    and t.movement_type = b.movement_type
