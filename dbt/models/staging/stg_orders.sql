select
    order_id,
    user_id,
    city          as city_name,
    order_status,
    total_amount,
    currency,
    placed_at,
    cancelled_at,
    order_date
from {{ source('lake_gold', 'orders_current') }}
