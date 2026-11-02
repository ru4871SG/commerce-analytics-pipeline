{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='order_key',
    on_schema_change='append_new_columns',
    tmp_relation_type='transient',
    post_hook="delete from {{ this }} where order_key not in (select order_key from {{ ref('fct_orders') }})",
) }}

{#
    Slowly changing dimension type 2 of the order state: one row per order and state it has been
    in, from valid_from (the updated_at of the change) to valid_to (the next change; NULL while it
    is the current state). Answers "what was this order's state on day X" and "how long do orders
    take to be refunded or cancelled", which fct_orders, holding only the current state, cannot.

    Built from RAW.CRM.ORDERS_CHANGES, which keeps every version of an order the loader has
    copied. A version that changed none of the state columns (only the total, say) is folded into
    the state before it. History starts at an order's first load: the seed is loaded once, as it
    stood on 30 September 2026, and every simulated day after it adds the changes it made.
    Same orders as fct_orders: no carts, no test accounts.

    Incremental: the change log only grows, so a run rebuilds the history of the orders that have
    a version loaded since the last run, from all of their versions, and replaces those orders'
    rows (delete+insert on order_key). The others are left as they are.
      * "Since the last run" reaches 24 hours behind the latest history_loaded_at. _LOADED_AT is
        when a load started, so a dbt run in the middle of a load can see one brand's versions
        before an earlier-started brand's land; the margin rebuilds those orders on the next run.
      * The post-hook deletes the history of orders no longer in fct_orders (deleted in the
        source and resynced, or now a test account), which delete+insert alone would keep.
      * A table built before history_loaded_at existed has no cutoff to read: its first run
        rebuilds every order and gains the column.
    The batch is staged in a transient table rather than dbt's default temporary one, which the
    emulator cannot create; dbt drops it after the run.
#}
{%- set has_cutoff = false -%}
{%- if is_incremental() -%}
    {%- set existing = adapter.get_columns_in_relation(this) | map(attribute='name') | map('lower') | list -%}
    {%- set has_cutoff = 'history_loaded_at' in existing -%}
{%- endif %}
with changed_orders as (

    select distinct brand, order_id
    from {{ ref('stg_crm__orders_changes') }}
    {% if has_cutoff %}
    where loaded_at > (
        select coalesce(dateadd(hour, -24, max(history_loaded_at)), '1970-01-01'::timestamp_ntz)
        from {{ this }}
    )
    {% endif %}

),

versions as (

    select
        v.*,
        {{ dbt_utils.generate_surrogate_key([
            'v.state', 'v.payment_state', 'v.shipping_state',
            'v.is_subscription_canceled', 'v.subscription_canceled_at', 'v.is_subscription_paused',
        ]) }}                                                               as state_hash,
        max(v.loaded_at) over (partition by v.brand, v.order_id)            as history_loaded_at
    from {{ ref('stg_crm__orders_changes') }} as v
    inner join changed_orders as c
        on v.brand = c.brand and v.order_id = c.order_id
    -- The same version copied by two loads is one version
    qualify row_number() over (partition by v.brand, v.order_id, v.updated_at order by v.loaded_at desc) = 1

),

changes as (

    select
        *,
        lag(state_hash) over (partition by brand, order_id order by updated_at) as previous_state_hash
    from versions

),

states as (

    select
        *,
        lead(updated_at) over (partition by brand, order_id order by updated_at) as next_change_at
    from changes
    where previous_state_hash is null or previous_state_hash <> state_hash

)

select
    {{ dbt_utils.generate_surrogate_key(['s.brand', 's.order_id', 's.updated_at']) }} as order_state_key,
    o.order_key,
    o.brand_key,
    s.brand,
    s.order_id,
    s.state,
    s.payment_state,
    s.shipping_state,
    s.is_subscription_canceled,
    s.subscription_canceled_at,
    s.is_subscription_paused,
    s.updated_at                                                        as valid_from,
    s.next_change_at                                                    as valid_to,
    s.next_change_at is null                                            as is_current,
    s.history_loaded_at
from states as s
inner join {{ ref('fct_orders') }} as o
    on s.brand = o.brand and s.order_id = o.order_id
