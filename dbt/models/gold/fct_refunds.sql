{{ config(materialized='table') }}

{#
    One row per refund payment. Refunds join to orders through order_number, within a brand,
    because the source application stores the number rather than the order id. reduces_revenue is
    the one definition of the refunds that come off revenue and contribution margin, on the day
    they were issued, as in the accounts: a closed month does not change when a refund arrives later.
    refund_amount_usd is at the brand's constant-currency rate; refund_amount_usd_actual at the ECB's
    rate of the day the refund was issued (int_fx_rates_daily).
#}
select
    {{ dbt_utils.generate_surrogate_key(['r.brand', 'r.refund_payment_id']) }} as refund_key,
    o.order_key,
    o.customer_key,
    o.brand_key,
    r.brand,
    r.refund_payment_id,
    r.order_number,
    r.refund_amount_cents,
    r.refund_amount_excl_vat,
    (r.refund_amount_excl_vat * o.usd_rate)::number(18, 6)      as refund_amount_usd,
    (r.refund_amount_excl_vat * fx.usd_per_unit)::number(18, 6) as refund_amount_usd_actual,
    r.state,
    r.state = 'completed' and o.is_revenue_order                as reduces_revenue,
    r.refund_reason,
    r.requires_return,
    r.issued_at,
    r.issued_date
from {{ ref('stg_crm__refund_payments') }} as r
inner join {{ ref('fct_orders') }} as o
    on r.brand = o.brand and r.order_number = o.order_number
left join {{ ref('int_fx_rates_daily') }} as fx
    on upper(o.currency_code) = fx.currency_code and r.issued_date = fx.date_day
