{#
    The date every rolling and to-date figure is measured from: var('as_of_date') when set,
    otherwise the latest order date in fct_orders. Returns a SQL expression of type date.
#}
{% macro as_of_date() -%}
    {%- if var('as_of_date') -%}
        to_date('{{ var("as_of_date") }}')
    {%- else -%}
        (select max(order_date) from {{ ref('fct_orders') }})
    {%- endif -%}
{%- endmacro %}

{#
    Amount in cents to the reporting currency unit, with VAT removed for brands whose prices
    include it. Pass the brands seed alias for the brand rules.
#}
{% macro amount_excl_vat(cents_expression, brands_alias='brands') -%}
    iff({{ brands_alias }}.prices_include_vat,
        {{ cents_expression }} / (1 + {{ brands_alias }}.vat_rate),
        {{ cents_expression }}) / 100
{%- endmacro %}

{#
    The business day of a UTC timestamp: its calendar date in var('business_timezone'). An order
    placed at 21:00 in New York is 01:00 UTC the next day, and belongs to the New York day.
#}
{% macro business_date(utc_timestamp) -%}
    convert_timezone('UTC', '{{ var("business_timezone") }}', {{ utc_timestamp }})::date
{%- endmacro %}

{# The UTC instant a business day starts at: midnight in var('business_timezone'). #}
{% macro business_day_start(day) -%}
    convert_timezone('{{ var("business_timezone") }}', 'UTC', ({{ day }})::timestamp_ntz)
{%- endmacro %}

{# The payment states that mean the order was paid: the one definition every model uses. #}
{% macro is_paid(payment_state) -%}
    coalesce({{ payment_state }}, '') in ('paid', 'partially_refunded', 'refunded')
{%- endmacro %}

{# A number spine 0..n, for date and day-offset scaffolds. #}
{% macro number_spine(n) -%}
    select generated_number - 1 as n
    from ({{ dbt_utils.generate_series(n + 1) }})
{%- endmacro %}

{#
    exec_daily_brand_metrics as one row per brand, day and metric, for the metrics that
    var('anomaly_min_difference') lists: what exec_kpi_anomalies compares day with day. Every
    value is a float, and cancellations are positive here (the daily table holds them negative,
    for its chart).
#}
{% macro daily_metric_values() -%}
    {%- for metric in var('anomaly_min_difference') %}
    select
        brand,
        brand_name,
        date_day,
        '{{ metric }}' as metric,
        ({{ '-canceled_subscriptions' if metric == 'canceled_subscriptions' else metric }})::float as value
    from {{ ref('exec_daily_brand_metrics') }}
    {%- if not loop.last %}
    union all
    {%- endif %}
    {%- endfor %}
{%- endmacro %}

{#
    The brands and months a monthly reporting table covers: every brand with an order, crossed
    with every month from the first order to the as-of date. They are the brands and months of
    exec_daily_brand_metrics. A table that starts from it has a row for a month with nothing in
    it, so its chart is never empty.
#}
{% macro brand_months() -%}
    select
        b.brand,
        b.brand_name,
        m.month_start
    from {{ ref('dim_brands') }} as b
    cross join (select distinct month_start from {{ ref('dim_dates') }}) as m
    where b.brand in (select o.brand from {{ ref('fct_orders') }} as o)
{%- endmacro %}

{#
    What moved monthly recurring revenue, one row per movement of a subscription. It adds its
    MRR in the month it starts (new, or reactivated when the customer had come back) and takes it
    away in the month it ends (churned, negative). Nothing after the as-of date moves a balance.
    A subscription that was never active (dim_subscriptions.ended_on = started_on) does both in
    one month. exec_mrr_movements and exec_mrr_monthly both start from these rows, so the two
    tables cannot tell different stories.
#}
{% macro subscription_movements() -%}
    select
        brand,
        date_trunc(month, started_on)::date                 as month_start,
        iff(is_reactivation, 'reactivated', 'new')          as movement_type,
        is_paused,
        mrr_usd,
        1                                                   as subscriptions
    from {{ ref('dim_subscriptions') }}
    where started_on <= {{ as_of_date() }}

    union all

    select
        brand,
        date_trunc(month, ended_on)::date                   as month_start,
        'churned'                                           as movement_type,
        is_paused,
        -mrr_usd                                            as mrr_usd,
        -1                                                  as subscriptions
    from {{ ref('dim_subscriptions') }}
    where ended_on <= {{ as_of_date() }}
{%- endmacro %}
