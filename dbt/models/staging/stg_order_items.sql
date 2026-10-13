select
    order_id,
    order_date,
    product_id,
    quantity,
    unit_price,
    line_amount
from {{ source('lake_gold', 'order_items') }}
