{{ config(materialized='table') }}

-- One row per brand. The dimension every fact joins to for the dashboard's brand filter, with
-- the USD rate the executive report converts the brand's amounts at.
select
    {{ dbt_utils.generate_surrogate_key(['b.brand']) }} as brand_key,
    b.brand,
    b.brand_name,
    b.currency_code,
    b.prices_include_vat,
    b.vat_rate,
    fx.usd_rate
from {{ ref('brands') }} as b
inner join {{ ref('fx_rates') }} as fx
    on b.currency_code = fx.currency_code
