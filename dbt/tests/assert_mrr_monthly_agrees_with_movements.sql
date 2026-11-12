with movements as (

    select
        brand,
        month_start,
        sum(iff(movement_type = 'new', mrr_usd, 0))                 as new_mrr_usd,
        sum(iff(movement_type = 'reactivated', mrr_usd, 0))         as reactivated_mrr_usd,
        -sum(iff(movement_type = 'churned', mrr_usd, 0))            as churned_mrr_usd,
        sum(iff(movement_type = 'new', subscriptions, 0))           as new_subscriptions,
        sum(iff(movement_type = 'reactivated', subscriptions, 0))   as reactivated_subscriptions,
        -sum(iff(movement_type = 'churned', subscriptions, 0))      as churned_subscriptions
    from {{ ref('exec_mrr_movements') }}
    group by brand, month_start

)

select
    coalesce(m.brand, v.brand)                      as brand,
    coalesce(m.month_start, v.month_start)          as month_start,
    m.new_mrr_usd,
    v.new_mrr_usd                                   as movements_new_mrr_usd,
    m.reactivated_mrr_usd,
    v.reactivated_mrr_usd                           as movements_reactivated_mrr_usd,
    m.churned_mrr_usd,
    v.churned_mrr_usd                               as movements_churned_mrr_usd,
    m.new_subscriptions,
    v.new_subscriptions                             as movements_new_subscriptions,
    m.reactivated_subscriptions,
    v.reactivated_subscriptions                     as movements_reactivated_subscriptions,
    m.churned_subscriptions,
    v.churned_subscriptions                         as movements_churned_subscriptions
from {{ ref('exec_mrr_monthly') }} as m
full outer join movements as v
    on m.brand = v.brand and m.month_start = v.month_start
where m.brand is null
   or v.brand is null
   or abs(m.new_mrr_usd - v.new_mrr_usd) > 0.005
   or abs(m.reactivated_mrr_usd - v.reactivated_mrr_usd) > 0.005
   or abs(m.churned_mrr_usd - v.churned_mrr_usd) > 0.005
   or m.new_subscriptions <> v.new_subscriptions
   or m.reactivated_subscriptions <> v.reactivated_subscriptions
   or m.churned_subscriptions <> v.churned_subscriptions
