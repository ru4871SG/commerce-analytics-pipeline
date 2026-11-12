{#
    Why orders are refunded. One row per brand, month and refund reason, for the refunds that
    come off revenue (completed, on a revenue order: fct_refunds.reduces_revenue), in the month
    they were issued, as revenue takes them: how many, their amount, how many needed the goods
    sent back, and the days from the order to the refund, summed, so the average is
    SUM(days_to_refund_sum) / SUM(refunds). Every reason has a row in every month, with zeros
    when nothing was refunded for it, so every column adds up and a chart is never empty. A
    refund with no reason is counted under 'Not given'. Amounts are in USD at constant-currency
    rates.
#}
with brand_months as (

    {{ brand_months() }}

),

refunds as (

    select
        r.brand,
        date_trunc(month, r.issued_date)::date              as month_start,
        coalesce(r.refund_reason, 'Not given')              as refund_reason,
        r.refund_amount_usd,
        r.requires_return,
        datediff(day, o.order_date, r.issued_date)          as days_to_refund
    from {{ ref('fct_refunds') }} as r
    inner join {{ ref('fct_orders') }} as o
        on r.order_key = o.order_key
    where r.reduces_revenue
      and r.issued_date <= {{ as_of_date() }}

),

reasons as (

    -- The reasons of every brand, so the chart's legend does not depend on the Brand filter
    select distinct refund_reason
    from refunds

    union all

    -- With no refund at all the table would have no row, and its charts no data
    select 'Not given' as refund_reason
    from (select count(*) as refunds from refunds) as counted
    where counted.refunds = 0

),

by_reason as (

    select
        brand,
        month_start,
        refund_reason,
        count(*)                                            as refunds,
        sum(refund_amount_usd)                              as refund_amount_usd,
        count(iff(requires_return, 1, null))                as refunds_requiring_return,
        sum(days_to_refund)                                 as days_to_refund_sum
    from refunds
    group by brand, month_start, refund_reason

)

-- Each column is cast to its contract type (_reporting.yml)
select
    m.brand::varchar                                        as brand,
    m.brand_name::varchar                                   as brand_name,
    m.month_start::date                                     as month_start,
    s.refund_reason::varchar                                as refund_reason,
    coalesce(r.refunds, 0)::int                             as refunds,
    coalesce(r.refund_amount_usd, 0)::float                 as refund_amount_usd,
    coalesce(r.refunds_requiring_return, 0)::int            as refunds_requiring_return,
    coalesce(r.days_to_refund_sum, 0)::int                  as days_to_refund_sum
from brand_months as m
cross join reasons as s
left join by_reason as r
    on m.brand = r.brand
    and m.month_start = r.month_start
    and s.refund_reason = r.refund_reason
