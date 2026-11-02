-- One row per refund. Refunds are linked to orders by order number, within a brand.
select
    lower(r._brand)                                          as brand,
    r.id                                                     as refund_payment_id,
    r.order_number,
    r.amount                                                 as refund_amount_cents,
    round({{ amount_excl_vat('r.amount', 'b') }}, 2)::number(18, 2)
                                                             as refund_amount_excl_vat,
    r.state,
    r.refund_reason,
    r.requires_return,
    r.issued_at,
    {{ business_date('r.issued_at') }}                       as issued_date
from {{ source('crm', 'refund_payments') }} as r
inner join {{ ref('brands') }} as b
    on lower(r._brand) = b.brand
