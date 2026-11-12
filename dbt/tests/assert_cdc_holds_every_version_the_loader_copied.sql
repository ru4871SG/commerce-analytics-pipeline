{{ config(severity='warn') }}

with captures as (

    select
        brand,
        max(captured_at)                                        as latest_capture,
        min(case when operation_code <> 'D' then updated_at end) as first_captured_change
    from {{ ref('stg_crm__orders_cdc') }}
    group by brand

),

captured as (

    select distinct
        brand,
        order_id,
        updated_at
    from {{ ref('stg_crm__orders_cdc') }}
    where operation_code <> 'D'

)

select
    v.brand,
    v.order_id,
    v.updated_at,
    v.loaded_at
from {{ ref('stg_crm__orders_changes') }} as v
inner join captures as c
    on v.brand = c.brand
left join captured as e
    on v.brand = e.brand and v.order_id = e.order_id and v.updated_at = e.updated_at
where v.loaded_at < c.latest_capture
    and v.updated_at > c.first_captured_change
    and e.order_id is null
