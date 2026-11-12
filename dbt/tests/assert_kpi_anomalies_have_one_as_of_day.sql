select
    brand,
    count(distinct iff(is_as_of_day, date_day, null))   as as_of_days,
    max(iff(is_as_of_day, date_day, null))              as as_of_day,
    max(date_day)                                       as latest_day
from {{ ref('exec_kpi_anomalies') }}
group by brand
having count(distinct iff(is_as_of_day, date_day, null)) <> 1
    or max(iff(is_as_of_day, date_day, null)) <> max(date_day)
    or max(date_day) <> {{ as_of_date() }}
