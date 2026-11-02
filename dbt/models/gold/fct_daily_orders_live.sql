{{ config(
    materialized=('table' if target.name == 'emulator' else 'dynamic_table'),
    target_lag='15 minutes',
    snowflake_warehouse=env_var('SNOWFLAKE_WAREHOUSE', 'TRANSFORMING_WH'),
) }}

{#
    Orders per brand and New York day, kept up to date by Snowflake itself. On Snowflake this is
    a dynamic table: it refreshes within target_lag of a change, so the dashboard's Live tiles
    (exec_daily_orders_live, a view over this table) move as soon as the merge task has applied a
    day's changes, before the dbt run that rebuilds gold. The local emulator has no dynamic tables,
    so there it is a normal table, rebuilt by every dbt run.

    It reads RAW.CRM.ORDERS only, which the merge task changes and nothing replaces, so Snowflake
    can refresh it incrementally: reading the silver view, which every dbt run replaces, would
    reinitialize it on every run, and the seeds, which every run reloads, would recompute it. The
    order rules come from the macros stg_crm__orders uses. It holds no brand settings (name, VAT,
    USD rate): exec_daily_orders_live applies them, so a brand change needs nothing here.
    dbt does not redeploy a dynamic table whose query alone has changed, so after editing this
    model on Snowflake run `dbt build --full-refresh --select fct_daily_orders_live` once.
#}
select
    lower(_brand)                                                           as brand,
    {{ business_date('created_at') }}                                       as date_day,
    count(*)::int                                                           as orders,
    sum(iff({{ is_paid('payment_state') }}, 1, 0))::int                     as paid_orders,
    sum(iff(coalesce(is_subscription, false) and {{ is_paid('payment_state') }}, 1, 0))::int
                                                                            as new_subscriptions,
    sum(iff(created_from_order_id is not null, 1, 0))::int                  as renewals,
    -- In cents of the brand's currency, VAT included where its prices include it
    sum(iff({{ is_paid('payment_state') }}, total, 0))::number(38, 0)       as paid_total_cents
from {{ source('crm', 'orders') }}
where state <> 'cart'
  and (customer_id is null or customer_id not in ({{ var('excluded_customer_ids') | join(', ') }}))
group by lower(_brand), {{ business_date('created_at') }}
