{#
    One row per change of an order that the change data capture read from PostgreSQL's
    write-ahead log (stg_crm__orders_cdc): every insert, update and delete, in the order they
    happened, with the state the order was in before.

    fct_order_state_history tells the same story from the loader's change log, which has one
    version per order and load: an order that changed twice between two loads shows its last
    state only, and one deleted in the source shows nothing. Here both are rows. A delete
    carries no column of its own (the log has the key only), so its state columns are NULL and
    previous_* say what was deleted.

    Carts and test accounts are included: this is the log as it was written. order_key is
    fct_orders' key, computed here and not joined: an order deleted in the source has events
    and no row in fct_orders, and nothing in the report depends on this table. The capture
    starts when a brand's replication slot is created, so the history before it is not here,
    and the table is empty until the first capture.
#}
with changes as (

    select
        *,
        row_number() over (partition by brand, order_id order by lsn_number)     as change_sequence,
        lag(state) over (partition by brand, order_id order by lsn_number)       as previous_state,
        lag(payment_state) over (partition by brand, order_id order by lsn_number)
                                                                                as previous_payment_state,
        lag(shipping_state) over (partition by brand, order_id order by lsn_number)
                                                                                as previous_shipping_state,
        lag(is_subscription_canceled) over (partition by brand, order_id order by lsn_number)
                                                                                as previous_is_subscription_canceled,
        lag(is_subscription_paused) over (partition by brand, order_id order by lsn_number)
                                                                                as previous_is_subscription_paused,
        lag(total_cents) over (partition by brand, order_id order by lsn_number) as previous_total_cents
    from {{ ref('stg_crm__orders_cdc') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['brand', 'order_id', 'lsn_number']) }}  as order_change_key,
    {{ dbt_utils.generate_surrogate_key(['brand', 'order_id']) }}                as order_key,
    brand,
    order_id,
    change_sequence,
    operation,
    operation_code = 'D'                                                        as is_delete,
    lsn,
    lsn_number,
    transaction_id,
    committed_at,
    state,
    payment_state,
    shipping_state,
    is_subscription_canceled,
    subscription_canceled_at,
    is_subscription_paused,
    total_cents,
    updated_at,
    previous_state,
    previous_payment_state,
    previous_shipping_state,
    previous_is_subscription_canceled,
    previous_is_subscription_paused,
    previous_total_cents,
    captured_at
from changes
