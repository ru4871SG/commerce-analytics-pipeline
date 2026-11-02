-- One row per brand: what it costs to process the first item of an order, and each item after it.
select
    lower(_brand)        as brand,
    first_item_amount    as first_item_processing_cents,
    additional_item_amount as additional_item_processing_cents
from {{ source('crm', 'processing_costs') }}
qualify row_number() over (partition by _brand order by id) = 1
