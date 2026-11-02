{#
    Customer acquisition cost and 90-day lifetime value per brand and channel. A customer's
    channel is the utm_source of their first order when the brand pays for that channel
    (fct_marketing_spend), and organic otherwise. Customers count when their first order falls in
    the 365 days that end 90 days before the as-of date, so each has had a full 90 days to order
    again; the ad spend is taken over the same days.
      cac_usd           ad spend / customers acquired. NULL for organic, which costs nothing.
      ltv_90d_usd       contribution margin of their orders in their first 90 days / customers
                        acquired: what is left after VAT, the cost of the order and that order's
                        refunds, which is what CAC is paid back from.
      ltv_to_cac_ratio  how many times their first 90 days paid back what they cost.
    One row per brand and channel, so a chart uses MAX for one brand; across brands AVG averages
    the brands' values, as the churn cards do. Amounts in USD at constant-currency rates.
#}
with acquisition_window as (

    select
        dateadd(day, -90 - 364, {{ as_of_date() }})::date             as window_start,
        dateadd(day, -90, {{ as_of_date() }})::date                   as window_end

),

paid_channels as (

    select distinct brand, utm_source as channel
    from {{ ref('fct_marketing_spend') }}

),

customers as (

    select
        c.brand,
        c.customer_id,
        c.first_order_date,
        coalesce(p.channel, 'organic')                                  as channel
    from {{ ref('dim_customers') }} as c
    cross join acquisition_window as w
    left join paid_channels as p
        on c.brand = p.brand and lower(c.attributed_utm_source) = p.channel
    where c.first_order_date between w.window_start and w.window_end

),

first_90_days as (

    select
        c.brand,
        c.customer_id,
        sum(o.contribution_margin_usd)                                  as contribution_margin_usd
    from customers as c
    inner join {{ ref('fct_orders') }} as o
        on c.brand = o.brand and c.customer_id = o.customer_id
    where o.is_revenue_order
      and o.order_date >= c.first_order_date
      and o.order_date < dateadd(day, 90, c.first_order_date)
    group by c.brand, c.customer_id

),

acquired as (

    select
        c.brand,
        c.channel,
        count(*)                                                        as acquired_customers,
        coalesce(sum(r.contribution_margin_usd), 0)                     as contribution_margin_90d_usd
    from customers as c
    left join first_90_days as r
        on c.brand = r.brand and c.customer_id = r.customer_id
    group by c.brand, c.channel

),

spend as (

    select
        s.brand,
        s.utm_source                                                    as channel,
        sum(s.spend_usd)                                                as spend_usd,
        sum(s.impressions)                                              as impressions,
        sum(s.clicks)                                                   as clicks
    from {{ ref('fct_marketing_spend') }} as s
    cross join acquisition_window as w
    where s.spend_date between w.window_start and w.window_end
    group by s.brand, s.utm_source

),

channels as (

    select brand, channel from acquired
    union
    select brand, channel from spend

),

measures as (

    select
        ch.brand,
        ch.channel,
        coalesce(s.spend_usd, 0)                                        as spend_usd,
        coalesce(s.impressions, 0)                                      as impressions,
        coalesce(s.clicks, 0)                                           as clicks,
        coalesce(a.acquired_customers, 0)                               as acquired_customers,
        coalesce(a.contribution_margin_90d_usd, 0)                      as contribution_margin_90d_usd,
        iff(s.spend_usd > 0, s.spend_usd / nullif(a.acquired_customers, 0), null)
                                                                        as cac,
        a.contribution_margin_90d_usd / nullif(a.acquired_customers, 0) as ltv_90d
    from channels as ch
    left join acquired as a
        on ch.brand = a.brand and ch.channel = a.channel
    left join spend as s
        on ch.brand = s.brand and ch.channel = s.channel

)

select
    m.brand,
    brands.brand_name,
    m.channel,
    w.window_start                                                      as acquisition_window_start,
    w.window_end                                                        as acquisition_window_end,
    m.spend_usd,
    m.impressions,
    m.clicks,
    m.acquired_customers,
    m.contribution_margin_90d_usd,
    round(m.cac, 2)::number(18, 2)                                      as cac_usd,
    round(m.ltv_90d, 2)::number(18, 2)                                  as ltv_90d_usd,
    round(m.ltv_90d / nullif(m.cac, 0), 3)::number(18, 3)               as ltv_to_cac_ratio
from measures as m
cross join acquisition_window as w
inner join {{ ref('brands') }} as brands
    on m.brand = brands.brand
