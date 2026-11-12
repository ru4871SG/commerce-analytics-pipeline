{{ config(materialized='table') }}

{#
    One row per non-cart order, all brands. Replaces the enriched order table the pandas ETL
    wrote back into each CRM database. Amounts are in cents; the rest are in currency units, with
    VAT removed where the brand's prices include it. gross_sales is what the order sold for;
    margin_before_refunds subtracts the cost of the order, as the report's "revenue" did; and
    contribution_margin also subtracts the completed refunds of this order, whenever they were
    issued. That is the view of an order or a customer (LTV, cohorts). Reports over a period take
    refunds on the day they were issued instead (fct_refunds). The *_usd columns convert these at the
    brand's constant-currency rate (seeds/fx_rates.csv), so brands in different currencies add up.
    usd_rate_actual and gross_sales_usd_actual use the ECB's rate of the order's own day instead
    (int_fx_rates_daily): what the order was worth in dollars when it was placed.
#}
with orders as (

    select
        o.*,
        b.brand_key,
        b.prices_include_vat,
        b.vat_rate,
        b.usd_rate,
        c.customer_key,
        c.first_order_date                                                   as customer_first_order_date,
        coalesce(startswith(o.acquisition_domain, 'pro.'), false)            as is_pro_domain
    from {{ ref('stg_crm__orders') }} as o
    inner join {{ ref('dim_brands') }} as b
        on o.brand = b.brand
    left join {{ ref('dim_customers') }} as c
        on o.brand = c.brand and o.customer_id = c.customer_id
    where o.state <> 'cart'
      and (o.customer_id is null or o.customer_id not in ({{ var('excluded_customer_ids') | join(', ') }}))

),

attribution_flags as (

    select
        *,
        created_from_order_id is not null                        as is_renewal,
        is_created_by_admin and created_from_order_id is null    as is_agent_order,
        is_pro_domain
            and coalesce(utm_source, '') = ''
            and coalesce(utm_medium, '') = ''
            and created_from_order_id is null
            and not is_created_by_admin                          as is_email_click
    from orders

),

rebuy_candidates as (

    select
        *,
        customer_id is not null and is_paid
            and not is_renewal and not is_agent_order and not is_email_click
                                                                 as is_rebuy_candidate
    from attribution_flags

),

rebuy_ranked as (

    select
        *,
        min(iff(is_rebuy_candidate, order_id, null))
            over (partition by brand, customer_id)               as first_rebuy_candidate_order_id
    from rebuy_candidates

),

costs as (

    select brand, order_id, cost_of_order_cents
    from {{ ref('int_order_costs') }}

),

first_items as (

    select brand, order_id, first_item_name, first_item_quantity, first_item_warehouse_sku
    from {{ ref('int_order_first_items') }}

),

refunds as (

    select
        brand,
        order_number,
        sum(refund_amount_excl_vat)::number(18, 2) as refunds_excl_vat
    from {{ ref('stg_crm__refund_payments') }}
    where state = 'completed'
      and order_number is not null
    group by brand, order_number

),

enriched as (

    select
        {{ dbt_utils.generate_surrogate_key(['r.brand', 'r.order_id']) }}          as order_key,
        r.brand_key,
        r.brand,
        r.order_id,
        r.order_number,
        r.customer_id,
        r.customer_key,
        r.created_from_order_id,
        case when r.created_from_order_id is not null
            then {{ dbt_utils.generate_surrogate_key(['r.brand', 'r.created_from_order_id']) }}
        end                                                                        as renewal_of_order_key,
        r.created_at,
        r.order_date,
        r.state,
        r.payment_state,
        r.shipping_state,
        r.checkout_completed_at,
        r.currency_code,
        r.total_cents,
        r.items_total_cents,
        r.adjustments_total_cents,
        coalesce(c.cost_of_order_cents, 0)::number(38, 0)                          as cost_of_order_cents,
        round({{ amount_excl_vat('r.total_cents', 'r') }}, 2)::number(18, 2)       as gross_sales,
        -- From the cents, not rounded to the cent per order: the same prices repeat, so what each
        -- order would lose to rounding adds up over a month
        ({{ amount_excl_vat('r.total_cents', 'r') }} * r.usd_rate)::number(18, 6)  as gross_sales_usd,
        -- At the rate of the order's day. NULL when no rate was published on or before it
        ({{ amount_excl_vat('r.total_cents', 'r') }} * fx.usd_per_unit)::number(18, 6)
                                                                                   as gross_sales_usd_actual,
        round(
            (iff(r.prices_include_vat, r.total_cents / (1 + r.vat_rate), r.total_cents)
                - coalesce(c.cost_of_order_cents, 0)) / 100,
            2
        )::number(18, 2)                                                           as margin_before_refunds,
        coalesce(rf.refunds_excl_vat, 0)::number(18, 2)                            as refunds_excl_vat,
        -- margin_before_refunds is the column defined just above (no joined relation has one)
        (margin_before_refunds - coalesce(rf.refunds_excl_vat, 0))::number(18, 2)  as contribution_margin,
        -- A missing state counts, as BLANK <> "cart" is true in DAX.
        coalesce(r.shipping_state, '') not in ('cart', 'cancelled')
            and coalesce(r.payment_state, '') <> 'awaiting_payment'                as is_revenue_order,
        r.is_paid,
        r.is_qualifying_order,
        r.is_subscription,
        -- A subscription starts with a paid order: one never paid is no subscription to report
        r.is_subscription and r.is_paid                                            as is_paid_subscription,
        r.is_subscription_canceled,
        r.subscription_canceled_at,
        r.cancel_date,
        floor(datediff(second, r.created_at, r.subscription_canceled_at) / 86400)::int
                                                                                   as churn_days,
        r.is_subscription_paused,
        r.is_subscription_paused and not r.is_subscription_canceled                as is_paused_excluding_cancelled,
        case
            when r.is_sales_funnel then iff(r.is_pro_domain, 'Email', 'Ads')
            when r.created_from_order_id is not null then 'Subscription'
            when r.is_created_by_admin then 'Calls'
            else 'Site'
        end                                                                        as acquisition_type,
        r.acquisition_domain,
        r.utm_source,
        r.utm_medium,
        r.utm_campaign,
        r.utm_content,
        r.utm_term,
        r.utm_id,
        case
            when r.is_renewal then 'Existing_customers'
            when r.is_agent_order then 'cs'
            when r.is_email_click then 'Klaviyo'
            when r.is_rebuy_candidate and r.order_id > r.first_rebuy_candidate_order_id
                then 'Existing_customers'
            else r.utm_source
        end                                                                        as attributed_utm_source,
        case
            when r.is_renewal then 'Subscription_renewal'
            when r.is_email_click then 'email'
            when r.is_rebuy_candidate and r.order_id > r.first_rebuy_candidate_order_id
                then 'Rebuy'
            else r.utm_medium
        end                                                                        as attributed_utm_medium,
        r.utm_term                                                                 as attributed_utm_term,
        fi.first_item_name,
        fi.first_item_quantity,
        fi.first_item_warehouse_sku,
        r.is_created_by_admin,
        r.usd_rate,
        fx.usd_per_unit::number(18, 6)                                             as usd_rate_actual,
        r.customer_first_order_date,
        datediff(day, r.customer_first_order_date, r.order_date)                   as days_since_customer_first_order,
        floor(datediff(day, r.customer_first_order_date, r.order_date) / 30)::int  as cohort_number
    from rebuy_ranked as r
    left join costs as c
        on r.brand = c.brand and r.order_id = c.order_id
    left join first_items as fi
        on r.brand = fi.brand and r.order_id = fi.order_id
    left join refunds as rf
        on r.brand = rf.brand and r.order_number = rf.order_number
    left join {{ ref('int_fx_rates_daily') }} as fx
        on upper(r.currency_code) = fx.currency_code and r.order_date = fx.date_day

)

select
    *,
    round(margin_before_refunds * usd_rate, 2)::number(18, 2)                  as margin_before_refunds_usd,
    round(contribution_margin * usd_rate, 2)::number(18, 2)                    as contribution_margin_usd
from enriched
