{#
    Cumulative contribution margin per customer for each monthly cohort of new customers: for
    customers whose first paid order was in a month, their contribution margin (each order's refunds
    go with it) up to 30-day period N, divided by how many of them there are. Periods count from each customer's own
    first paid order. Feeds the cohort matrix (rows: first-order month, columns: period N). A
    period shows once every customer of the month has finished it, so a recent month's latest
    periods stay hidden until its youngest customer gets there.
    Across brands the report shows the plain average of the brands' values: use AVG.
    Amounts are in USD at constant-currency rates.
#}
with customers as (

    select brand, customer_id, first_order_month, first_order_date
    from {{ ref('dim_customers') }}
    where first_order_month is not null

),

margin_by_period as (

    select
        c.brand,
        c.first_order_month,
        o.cohort_number,
        sum(o.contribution_margin_usd)                  as contribution_margin_usd
    from customers as c
    inner join {{ ref('fct_orders') }} as o
        on c.brand = o.brand and c.customer_id = o.customer_id
    where o.is_revenue_order
      and o.cohort_number >= 0
    group by c.brand, c.first_order_month, o.cohort_number

),

months as (

    select
        brand,
        first_order_month,
        max(first_order_date)               as latest_first_order_date,
        count(*)                            as new_customers
    from customers
    group by brand, first_order_month

),

periods as (

    {{ number_spine(300) }}

),

grid as (

    select
        m.brand,
        m.first_order_month,
        m.new_customers,
        p.n                                 as cohort_number
    from months as m
    cross join periods as p
    -- Only periods every customer has finished. Period n ends on day 30 x (n + 1) - 1 of a
    -- customer's clock, and the youngest customer of the month gets there last.
    where 30 * (p.n + 1) - 1 <= datediff(day, m.latest_first_order_date, {{ as_of_date() }})

),

cumulative as (

    select
        g.brand,
        g.first_order_month,
        g.cohort_number,
        g.new_customers,
        coalesce(r.contribution_margin_usd, 0)      as period_contribution_margin_usd,
        sum(coalesce(r.contribution_margin_usd, 0)) over (
            partition by g.brand, g.first_order_month
            order by g.cohort_number
            rows between unbounded preceding and current row
        )                                           as cumulative_contribution_margin_usd
    from grid as g
    left join margin_by_period as r
        on g.brand = r.brand
        and g.first_order_month = r.first_order_month
        and g.cohort_number = r.cohort_number

)

-- Each column is cast to its contract type (_reporting.yml)
select
    c.brand::varchar                                                    as brand,
    brands.brand_name::varchar                                          as brand_name,
    c.first_order_month::date                                           as first_order_month,
    c.cohort_number::int                                                as cohort_number,
    c.new_customers::int                                                as new_customers,
    c.period_contribution_margin_usd::float                             as period_contribution_margin_usd,
    c.cumulative_contribution_margin_usd::float                         as cumulative_contribution_margin_usd,
    (c.cumulative_contribution_margin_usd / c.new_customers)::float     as contribution_margin_per_customer_usd
from cumulative as c
inner join {{ ref('brands') }} as brands
    on c.brand = brands.brand
