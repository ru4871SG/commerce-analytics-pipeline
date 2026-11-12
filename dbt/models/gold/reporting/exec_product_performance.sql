{#
    Which products make the margin. One row per brand, month, product and purchase type, for the
    lines of revenue orders placed in that month: units, what the lines sold for, what they cost
    and the margin left. Every product the brand has in its catalogue has a row in every month,
    with zeros when it did not sell, so every column adds up and a chart is never empty.

    A line's revenue is its own total (units at their price, less the line's own discount), VAT
    removed. Shipping charged and discounts on the whole order are not spread over the lines, so
    the lines add up to the orders' item totals, not to gross sales. A line's cost follows the
    cost rule (int_order_item_costs): the first line of an order carries the order's fixed costs
    (shipping, processing, payment fees), and a line of a product with no cost SKU in the
    catalogue costs nothing, so its margin is its revenue. Refunds are not subtracted here:
    units_in_refunded_orders counts the units of orders that had a completed refund, in the
    month of the order, so an earlier month can grow when a refund arrives later. Amounts are in
    USD at constant-currency rates.
#}
with brand_months as (

    {{ brand_months() }}

),

products as (

    select distinct
        brand,
        product_name,
        purchase_type
    from {{ ref('dim_products') }}

),

refunded_orders as (

    select distinct order_key
    from {{ ref('fct_refunds') }}
    where reduces_revenue

),

lines as (

    select
        i.brand,
        date_trunc(month, i.order_date)::date                                   as month_start,
        p.product_name,
        p.purchase_type,
        sum(i.quantity)                                                         as units,
        sum({{ amount_excl_vat('i.item_total_cents', 'b') }} * b.usd_rate)      as line_revenue_usd,
        sum(i.item_cost_cents / 100 * b.usd_rate)                               as line_cost_usd,
        sum(iff(r.order_key is not null, i.quantity, 0))                        as units_in_refunded_orders
    from {{ ref('fct_order_items') }} as i
    inner join {{ ref('fct_orders') }} as o
        on i.order_key = o.order_key
    inner join {{ ref('dim_products') }} as p
        on i.product_variant_key = p.product_variant_key
    inner join {{ ref('dim_brands') }} as b
        on i.brand_key = b.brand_key
    left join refunded_orders as r
        on i.order_key = r.order_key
    where o.is_revenue_order
      and i.order_date <= {{ as_of_date() }}
    group by i.brand, date_trunc(month, i.order_date)::date, p.product_name, p.purchase_type

)

-- Each column is cast to its contract type (_reporting.yml)
select
    m.brand::varchar                                                        as brand,
    m.brand_name::varchar                                                   as brand_name,
    m.month_start::date                                                     as month_start,
    p.product_name::varchar                                                 as product_name,
    p.purchase_type::varchar                                                as purchase_type,
    coalesce(l.units, 0)::int                                               as units,
    coalesce(l.line_revenue_usd, 0)::float                                  as line_revenue_usd,
    coalesce(l.line_cost_usd, 0)::float                                     as line_cost_usd,
    (coalesce(l.line_revenue_usd, 0) - coalesce(l.line_cost_usd, 0))::float as line_margin_usd,
    coalesce(l.units_in_refunded_orders, 0)::int                            as units_in_refunded_orders
from brand_months as m
inner join products as p
    on m.brand = p.brand
left join lines as l
    on m.brand = l.brand
    and m.month_start = l.month_start
    and p.product_name = l.product_name
    and p.purchase_type = l.purchase_type
