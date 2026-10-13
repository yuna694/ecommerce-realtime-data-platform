select
    p.product_id,
    p.product_name,
    p.category,
    sum(i.quantity)      as units_sold,
    sum(i.line_amount)   as revenue,
    rank() over (order by sum(i.quantity) desc) as rank_by_units
from {{ ref('fact_order_items') }} i
join {{ ref('dim_product') }} p on p.product_id = i.product_id
where i.is_revenue
group by p.product_id, p.product_name, p.category
