{{ config(materialized='view') }}

{#
    Orders and sales per brand and day, for the dashboard's Live tiles. A view over
    fct_daily_orders_live, the dynamic table Snowflake keeps up to date after each merge, so the
    tiles move before the dbt run that rebuilds gold. The brand's name, VAT and USD rate are applied
    here, when the dashboard reads it, so a change to the brand registry or the rates shows at once.
    Sales are paid order totals without VAT, in USD at constant-currency rates, before refunds and
    the cost of the order (revenue_usd and contribution_margin_usd in exec_daily_brand_metrics
    subtract them).
#}
-- Each column is cast to its contract type (_reporting.yml)
select
    l.brand::varchar                                                    as brand,
    b.brand_name::varchar                                               as brand_name,
    l.date_day::date                                                    as date_day,
    l.orders::int                                                       as orders,
    l.paid_orders::int                                                  as paid_orders,
    l.new_subscriptions::int                                            as new_subscriptions,
    l.renewals::int                                                     as renewals,
    round({{ amount_excl_vat('l.paid_total_cents', 'b') }} * b.usd_rate, 2)::number(18, 2)
                                                                        as sales_usd
from {{ ref('fct_daily_orders_live') }} as l
inner join {{ ref('dim_brands') }} as b
    on l.brand = b.brand
