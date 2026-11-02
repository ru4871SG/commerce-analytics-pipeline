{#
    Decides which schema each model is built in.
    prod and emulator: the custom schema from dbt_project.yml (silver, gold), so Superset and
                       the DAG always read ANALYTICS.SILVER and ANALYTICS.GOLD.
    dev:               <target schema>_<custom schema>, so developers never collide.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- elif target.name in ['prod', 'emulator'] -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
