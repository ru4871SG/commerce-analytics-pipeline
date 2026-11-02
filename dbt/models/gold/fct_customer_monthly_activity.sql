{{ config(materialized='table') }}

{#
    Periodic snapshot: one row per brand and customer per calendar month since their first
    order, saying whether they ordered again that month. Month 0 is the month of the first
    order and rows continue to the as-of month. Replaces the unique_orders_<brand> tables.
#}
with customers as (

    select customer_key, brand_key, brand, customer_id, first_order_month
    from {{ ref('dim_customers') }}
    where first_order_month is not null

),

active_months as (

    select distinct
        o.customer_key,
        datediff(month, c.first_order_month, date_trunc(month, o.order_date)) as cohort_number
    from {{ ref('fct_orders') }} as o
    inner join customers as c
        on o.customer_key = c.customer_key
    where o.is_qualifying_order
      and o.customer_id is not null

),

month_numbers as (

    {{ number_spine(600) }}

),

as_of as (

    select date_trunc(month, {{ as_of_date() }}) as as_of_month

)

select
    {{ dbt_utils.generate_surrogate_key(['c.brand', 'c.customer_id', 'm.n']) }} as customer_month_key,
    c.customer_key,
    c.brand_key,
    c.brand,
    c.customer_id,
    c.first_order_month                                  as cohort_start_month,
    m.n                                                  as cohort_number,
    dateadd(month, m.n, c.first_order_month)::date       as activity_month,
    a.customer_key is not null                           as has_activity
from customers as c
cross join month_numbers as m
cross join as_of as d
left join active_months as a
    on c.customer_key = a.customer_key and m.n = a.cohort_number
where dateadd(month, m.n, c.first_order_month) <= d.as_of_month
