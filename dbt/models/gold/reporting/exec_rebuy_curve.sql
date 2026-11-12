{#
    Share of a brand's customers who had ordered again within N days of their first order,
    N = 0..curve_max_day. Feeds the "Rebuy %" curve.
#}
-- Each column is cast to its contract type (_reporting.yml)
select
    r.brand::varchar                                        as brand,
    brands.brand_name::varchar                              as brand_name,
    r.day_offset::int                                       as day_offset,
    sum(r.rebuyers)::int                                    as rebuyers,
    sum(r.total_customers)::int                             as customers,
    (sum(r.rebuyers) / sum(r.total_customers))::float       as rebuy_rate
from {{ ref('fct_rebuy_by_first_product') }} as r
inner join {{ ref('brands') }} as brands
    on r.brand = brands.brand
group by r.brand, brands.brand_name, r.day_offset
