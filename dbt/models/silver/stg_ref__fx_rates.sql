-- One row per currency and day the ECB published a rate, with what one unit of the currency was
-- worth in USD that day. The history comes from the seed fx_rates_ecb, the days after it
-- from RAW.REF.FX_RATES, which the FX side load appends to. The ECB quotes every currency per
-- EUR, so dollars per unit are the USD rate divided by the currency's own.
-- The report of a past day does not depend on what was loaded since. Source: ECB statistics.
with published as (

    select
        rate_date,
        upper(currency_code)                    as currency_code,
        eur_rate,
        1                                       as preference,
        null::timestamp_ntz                     as loaded_at
    from {{ ref('fx_rates_ecb') }}

    union all

    select
        rate_date,
        upper(currency_code)                    as currency_code,
        eur_rate,
        2                                       as preference,
        _loaded_at                              as loaded_at
    from {{ source('ref', 'fx_rates') }}

),

one_per_day as (

    -- A rate that is not positive cannot be divided by: it is left out, and the day before it
    -- is carried forward
    select rate_date, currency_code, eur_rate
    from published
    where eur_rate > 0
    qualify row_number() over (
        partition by currency_code, rate_date
        order by preference, loaded_at, eur_rate
    ) = 1

),

dollars as (

    select rate_date, eur_rate as usd_per_eur
    from one_per_day
    where currency_code = 'USD'

)

select
    r.rate_date,
    r.currency_code,
    r.eur_rate,
    d.usd_per_eur,
    iff(r.currency_code = 'USD', 1, d.usd_per_eur / r.eur_rate)::number(18, 6)  as usd_per_unit
from one_per_day as r
inner join dollars as d
    on r.rate_date = d.rate_date
