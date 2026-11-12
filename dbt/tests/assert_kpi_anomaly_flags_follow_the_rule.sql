with rows_with_rule as (

    select
        *,
        value - baseline_median                                                 as difference,
        case metric
            {%- for metric, minimum in var('anomaly_min_difference').items() %}
            when '{{ metric }}' then {{ minimum }}
            {%- endfor %}
        end                                                                     as min_difference
    from {{ ref('exec_kpi_anomalies') }}

),

expected as (

    select
        *,
        coalesce(
            comparison_days >= {{ var('anomaly_min_comparison_days') }}
            and abs(difference) >= min_difference
            and (spread = 0 or abs(robust_z) >= {{ var('anomaly_z_threshold') }}),
            false
        )                                                                       as expected_is_anomaly,
        round(difference / nullif(spread, 0), 2)                                as expected_robust_z,
        case when difference > 0 then 'above' when difference < 0 then 'below' end
                                                                                as expected_direction
    from rows_with_rule

)

select brand, metric, date_day, value, baseline_median, spread, robust_z, direction, is_anomaly, comparison_days
from expected
where min_difference is null
   or is_anomaly <> expected_is_anomaly
   or coalesce(robust_z, -999999) <> coalesce(expected_robust_z, -999999)
   or coalesce(direction, 'none') <> coalesce(expected_direction, 'none')
   -- No baseline: nothing to compare with, so nothing may be said about the day
   or (comparison_days = 0 and (baseline_median is not null or spread is not null or is_anomaly))
