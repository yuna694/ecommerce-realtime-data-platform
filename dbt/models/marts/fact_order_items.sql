-- Grain: one row per order line.
select
    i.order_id,
    i.product_id,
    f.date_key,
    f.city_key,
    i.quantity,
    i.unit_price,
    i.line_amount,
    f.is_revenue
from {{ ref('stg_order_items') }} i
join {{ ref('fact_orders') }} f on f.order_id = i.order_id
