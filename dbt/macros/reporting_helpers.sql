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
