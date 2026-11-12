with months as (

    select
        *,
        lag(month_start) over (partition by brand order by month_start)             as previous_month_start,
        lag(closing_mrr_usd) over (partition by brand order by month_start)         as previous_closing_mrr_usd,
        lag(closing_subscriptions) over (partition by brand order by month_start)   as previous_closing_subscriptions
    from {{ ref('exec_mrr_monthly') }}

)

select
    brand,
    month_start,
    previous_month_start,
    previous_closing_mrr_usd,
    opening_mrr_usd,
    new_mrr_usd,
    reactivated_mrr_usd,
    churned_mrr_usd,
    closing_mrr_usd,
    previous_closing_subscriptions,
    opening_subscriptions,
    new_subscriptions,
    reactivated_subscriptions,
    churned_subscriptions,
    closing_subscriptions
from months
where abs(opening_mrr_usd + new_mrr_usd + reactivated_mrr_usd - churned_mrr_usd - closing_mrr_usd) > 0.005
   or opening_subscriptions + new_subscriptions + reactivated_subscriptions - churned_subscriptions
       <> closing_subscriptions
   or abs(opening_mrr_usd - coalesce(previous_closing_mrr_usd, 0)) > 0.005
   or opening_subscriptions <> coalesce(previous_closing_subscriptions, 0)
   or previous_month_start <> dateadd(month, -1, month_start)::date
