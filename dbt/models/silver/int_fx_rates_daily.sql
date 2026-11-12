-- A table, not a view: the orders and the refunds both join it, and a view would carry the rates
-- forward again for each.
{{ config(materialized='table') }}

{#
    One row per currency and calendar day, from the first published rate to the latest order or
    refund: the rate to convert that day's amounts at. The ECB publishes no rate on a weekend or
    a TARGET holiday, so such a day takes the latest rate published before it; rate_date says
    which, and days_carried_forward how old it is (0 on a day with its own rate, up to 4 over
    Easter). Only a rate dated on or before the day is ever used, never a later one: a rate
    published this afternoon cannot change a report already built for yesterday. It reads the
    staging models, not fct_orders, which joins it.
#}
with rates as (

    select rate_date, currency_code, eur_rate, usd_per_unit
    from {{ ref('stg_ref__fx_rates') }}

),

last_days as (

    select
        (select max(order_date) from {{ ref('stg_crm__orders') }})              as last_order_day,
        (select max(issued_date) from {{ ref('stg_crm__refund_payments') }})    as last_refund_day

),

bounds as (

    -- greatest() of a NULL is NULL: with no refund yet, the last order alone ends the calendar
    select
        (select min(rate_date) from rates)                                      as first_day,
        greatest(last_order_day, coalesce(last_refund_day, last_order_day))     as last_day
    from last_days

),

days as (

    select dateadd(day, s.n, b.first_day)::date as date_day
    from bounds as b
    cross join ({{ number_spine(10000) }}) as s
    where dateadd(day, s.n, b.first_day) <= b.last_day

),

carried as (

    -- The latest day up to this one that has a rate. A maximum skips the days without one, on
    -- Snowflake and on the emulator alike, where ignore nulls is not written the same way
    select
        c.currency_code,
        d.date_day,
        max(r.rate_date) over (
            partition by c.currency_code order by d.date_day
            rows between unbounded preceding and current row
        )                                                                       as rate_date
    from (select distinct currency_code from rates) as c
    cross join days as d
    left join rates as r
        on c.currency_code = r.currency_code and d.date_day = r.rate_date

)

select
    c.currency_code,
    c.date_day,
    c.rate_date,
    datediff(day, c.rate_date, c.date_day)::int                                 as days_carried_forward,
    r.eur_rate,
    r.usd_per_unit
from carried as c
inner join rates as r
    on c.currency_code = r.currency_code and c.rate_date = r.rate_date
