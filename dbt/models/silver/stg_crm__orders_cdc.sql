-- Every change of an order that PostgreSQL wrote to its write-ahead log since the brand's
-- replication slot exists, as wal2json decoded it. one
-- row per insert, update and delete. An insert or update carries the whole row as it was
-- after the change, and a delete carries the order's id only.
select
    lower(_brand)                                    as brand,
    id                                               as order_id,
    _op                                              as operation_code,
    case _op
        when 'I' then 'insert'
        when 'U' then 'update'
        when 'D' then 'delete'
    end                                              as operation,
    _lsn                                             as lsn,
    _lsn_number                                      as lsn_number,
    _xid                                             as transaction_id,
    _committed_at                                    as committed_at,
    state,
    payment_state,
    shipping_state,
    is_subscription_canceled,
    subscription_canceled_at,
    subscription_paused                              as is_subscription_paused,
    total                                            as total_cents,
    updated_at,
    _loaded_at                                       as captured_at
from {{ source('crm', 'orders_cdc') }}
