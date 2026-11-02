{#
    Share of a brand's customers who had ordered again within N days of their first order,
    N = 0..curve_max_day. Feeds the "Rebuy %" curve.
#}
select
    r.brand,
    brands.brand_name,
    r.day_offset,
    sum(r.rebuyers)                                 as rebuyers,
    sum(r.total_customers)                          as customers,
    sum(r.rebuyers) / sum(r.total_customers)        as rebuy_rate
from {{ ref('fct_rebuy_by_first_product') }} as r
inner join {{ ref('brands') }} as brands
    on r.brand = brands.brand
group by r.brand, brands.brand_name, r.day_offset
