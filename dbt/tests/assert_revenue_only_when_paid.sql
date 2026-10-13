-- Business rule: revenue must never be booked for cancelled or unpaid orders.
select order_id from {{ ref('fact_orders') }}
where revenue_amount > 0 and (order_status <> 'placed' or payment_status <> 'success')
