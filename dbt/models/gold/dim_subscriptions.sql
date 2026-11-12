{{ config(materialized='table') }}

{#
    One row per paid subscription: a subscription order that was paid
    (fct_orders.is_paid_subscription; one never paid never started). The row is the
    subscription's first order, so subscription_key is that order's order_key, and its renewals
    are the orders whose renewal_of_order_key points at it.

    started_on and ended_on follow exec_active_subscriptions_weekly: a subscription runs from its
    order date until its cancellation date, and one cancelled with no date, or before it started,
    ends on the day it started and is never active.

    What it is worth each month, mrr_usd, is one unit of its Subscribe line at that line's unit
    price, VAT removed, in USD at the brand's constant-currency rate: what a renewal charges. The
    first order's extra units, its upsell line, the shipping charged and line discounts are not
    recurring. A subscription renews about every 30 days and is treated as monthly.
#}
with subscriptions as (

    select
        o.order_key                                                         as subscription_key,
        o.brand_key,
        o.customer_key,
        o.brand,
        o.order_id,
        o.order_number,
        o.customer_id,
        o.currency_code,
        o.order_date                                                        as started_on,
        -- Cancelled with no date, or before it started: never counted as active
        iff(o.is_subscription_canceled, greatest(o.order_date, coalesce(o.cancel_date, o.order_date)), null)
                                                                            as ended_on,
        o.is_subscription_paused                                            as is_paused
    from {{ ref('fct_orders') }} as o
    where o.is_paid_subscription

),

order_lines as (

    -- The line that renews comes first: the Subscribe line of the first order, or the order's
    -- first line when it has none (the line the shop renews)
    select
        i.order_key,
        i.product_variant_key,
        i.product_name,
        i.variant_code,
        i.unit_price_cents,
        row_number() over (
            partition by i.order_key
            order by iff(p.purchase_type = 'Subscribe', 0, 1), i.order_item_id
        )                                                                   as line_rank
    from {{ ref('fct_order_items') }} as i
    inner join {{ ref('dim_products') }} as p
        on i.product_variant_key = p.product_variant_key

),

renewals as (

    -- A renewal order that was cancelled, or still awaits payment, is not a renewal
    select
        r.renewal_of_order_key                                              as subscription_key,
        count(*)                                                            as renewals_count,
        max(r.order_date)                                                   as last_renewal_date
    from {{ ref('fct_orders') }} as r
    where r.renewal_of_order_key is not null
      and r.is_paid
      and r.order_date <= {{ as_of_date() }}
    group by r.renewal_of_order_key

),

earlier as (

    -- The customer's earlier subscriptions, and how many of them were still running on the day
    -- this one started
    select
        s.subscription_key,
        count(*)                                                            as earlier_subscriptions,
        count(iff(e.ended_on is null or e.ended_on > s.started_on, 1, null))
                                                                            as earlier_still_running
    from subscriptions as s
    inner join subscriptions as e
        on s.brand = e.brand
        and s.customer_id = e.customer_id
        and (e.started_on < s.started_on or (e.started_on = s.started_on and e.order_id < s.order_id))
    group by s.subscription_key

)

select
    s.subscription_key,
    s.brand_key,
    s.customer_key,
    l.product_variant_key,
    s.brand,
    s.order_id,
    s.order_number,
    s.customer_id,
    l.product_name,
    l.variant_code,
    s.started_on,
    s.ended_on,
    s.started_on <= {{ as_of_date() }}
        and (s.ended_on is null or s.ended_on > {{ as_of_date() }})          as is_active,
    s.is_paused,
    -- Back after a break: the customer had subscribed before, and none of those was still running
    coalesce(e.earlier_subscriptions, 0) > 0
        and coalesce(e.earlier_still_running, 0) = 0                        as is_reactivation,
    coalesce(r.renewals_count, 0)::int                                      as renewals_count,
    r.last_renewal_date,
    s.currency_code,
    round({{ amount_excl_vat('l.unit_price_cents', 'b') }}, 2)::number(18, 2)
                                                                            as recurring_amount,
    -- From the cents, not rounded to the cent per subscription: the same prices repeat, so what
    -- each would lose to rounding adds up over a brand
    ({{ amount_excl_vat('l.unit_price_cents', 'b') }} * b.usd_rate)::number(18, 6)
                                                                            as mrr_usd
from subscriptions as s
inner join {{ ref('dim_brands') }} as b
    on s.brand = b.brand
left join order_lines as l
    on s.subscription_key = l.order_key and l.line_rank = 1
left join renewals as r
    on s.subscription_key = r.subscription_key
left join earlier as e
    on s.subscription_key = e.subscription_key
