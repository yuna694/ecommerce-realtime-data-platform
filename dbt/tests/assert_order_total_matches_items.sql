-- Reconciliation: an order's total must equal the sum of its lines (tolerance 1 cent).
select o.order_id
from {{ ref('fact_orders') }} o
join (select order_id, sum(line_amount) as items_total
      from {{ ref('fact_order_items') }} group by order_id) i using (order_id)
where abs(o.total_amount - i.items_total) > 0.01
