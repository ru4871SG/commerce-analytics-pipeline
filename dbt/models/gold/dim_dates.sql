{{ config(materialized='table') }}

{#
    One row per calendar day, from the first order date to the as-of date. Facts store
    order_date / issued_date as a date and join here; no to-date flags are stored because they
    depend on the as-of date, which the reporting models apply themselves.
#}
with bounds as (

    select min(order_date) as first_day, {{ as_of_date() }} as as_of_date
    from {{ ref('fct_orders') }}

),

days as (

    select dateadd(day, s.n, b.first_day)::date as date_day
    from bounds as b
    cross join ({{ number_spine(10000) }}) as s
    where dateadd(day, s.n, b.first_day) <= b.as_of_date

)

select
    date_day,
    date_trunc(week, date_day)::date       as week_start,
    date_trunc(month, date_day)::date      as month_start,
    quarter(date_day)                      as quarter,
    year(date_day)                         as year,
    date_part(weekiso, date_day)           as iso_week,
    date_part(dayofweek, date_day)         as day_of_week,
    dayname(date_day)                      as day_name,
    monthname(date_day)                    as month_name
from days
