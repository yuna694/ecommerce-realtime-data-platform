-- Grain: one row per order.
-- Business rule: an order counts as revenue only if it is still 'placed' (not cancelled)
-- AND its latest payment attempt succeeded.
select
    o.order_id,
    o.user_id                                     as customer_id,
    c.city_key,
    o.order_date                                  as date_key,
    o.placed_at,
    o.cancelled_at,
    o.order_status,
    coalesce(p.payment_status, 'no_payment')      as payment_status,
    coalesce(p.payment_attempts, 0)               as payment_attempts,
    o.total_amount,
    (o.order_status = 'placed' and p.payment_status = 'success')                       as is_revenue,
    case when o.order_status = 'placed' and p.payment_status = 'success'
         then o.total_amount else 0 end                                                as revenue_amount
from {{ ref('stg_orders') }} o
left join {{ ref('stg_payments') }} p on p.order_id = o.order_id
left join {{ ref('dim_city') }} c     on c.city_name = o.city_name
