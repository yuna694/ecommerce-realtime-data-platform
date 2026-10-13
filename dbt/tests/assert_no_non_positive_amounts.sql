-- Fails if any order or order line has a zero/negative amount (should be quarantined upstream).
select order_id from {{ ref('fact_orders') }} where total_amount <= 0
union all
select order_id from {{ ref('fact_order_items') }} where line_amount <= 0
