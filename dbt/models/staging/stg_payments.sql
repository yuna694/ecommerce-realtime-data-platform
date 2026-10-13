select
    order_id,
    payment_status,
    payment_amount,
    payment_method,
    payment_attempts,
    last_payment_at
from {{ source('lake_gold', 'payments_current') }}
