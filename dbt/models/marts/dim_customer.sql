select
    user_id                                              as customer_id,
    min(placed_at)                                       as first_order_at,
    count(*)                                             as orders_count,
    count(*) filter (where order_status = 'cancelled')   as cancelled_orders
from {{ ref('stg_orders') }}
group by user_id
