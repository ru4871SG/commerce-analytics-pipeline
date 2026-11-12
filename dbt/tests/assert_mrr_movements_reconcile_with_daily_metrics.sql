with daily as (

    select
        brand,
        date_trunc(month, date_day)::date               as month_start,
        sum(new_subscriptions)                          as new_subscriptions,
        -sum(canceled_subscriptions)                    as canceled_subscriptions
    from {{ ref('exec_daily_brand_metrics') }}
    group by brand, date_trunc(month, date_day)::date

),

moved_cancellations as (

    -- Counted here in the month the subscription started...
    select
        brand,
        date_trunc(month, order_date)::date             as month_start,
        1                                               as cancellations
    from {{ ref('fct_orders') }}
    where is_paid_subscription
      and is_subscription_canceled
      and (cancel_date is null or cancel_date < order_date)
      and order_date <= {{ as_of_date() }}

    union all

    select
        brand,
        date_trunc(month, cancel_date)::date            as month_start,
        -1                                              as cancellations
    from {{ ref('fct_orders') }}
    where is_paid_subscription
      and is_subscription_canceled
      and cancel_date < order_date

),

moved as (

    select brand, month_start, sum(cancellations) as cancellations
    from moved_cancellations
    group by brand, month_start

),

movements as (

    select
        brand,
        month_start,
        sum(iff(movement_type = 'churned', 0, subscriptions))       as started_subscriptions,
        -sum(iff(movement_type = 'churned', subscriptions, 0))      as churned_subscriptions
    from {{ ref('exec_mrr_movements') }}
    group by brand, month_start

)

select
    coalesce(d.brand, v.brand)                          as brand,
    coalesce(d.month_start, v.month_start)              as month_start,
    d.new_subscriptions,
    v.started_subscriptions,
    d.canceled_subscriptions,
    coalesce(c.cancellations, 0)                        as cancellations_the_daily_table_cannot_place,
    v.churned_subscriptions
from daily as d
left join moved as c
    on d.brand = c.brand and d.month_start = c.month_start
full outer join movements as v
    on d.brand = v.brand and d.month_start = v.month_start
where d.brand is null
   or v.brand is null
   or v.started_subscriptions <> d.new_subscriptions
   or v.churned_subscriptions <> d.canceled_subscriptions + coalesce(c.cancellations, 0)
