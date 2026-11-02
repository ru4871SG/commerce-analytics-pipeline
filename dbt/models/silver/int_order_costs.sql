{#
    CostOfOrder, in cents, for every non-cart order: the sum of its item costs
    (int_order_item_costs). Items without a product cost contribute nothing.
#}
select
    brand,
    order_id,
    coalesce(sum(item_cost_cents), 0)::number(38, 0) as cost_of_order_cents
from {{ ref('int_order_item_costs') }}
group by brand, order_id
