select
    f.date_key,
    c.city_name,
    c.region,
    count(*)                              as orders,
    count(*) filter (where f.is_revenue)  as paid_orders,
    sum(f.revenue_amount)                 as revenue
from {{ ref('fact_orders') }} f
join {{ ref('dim_city') }} c on c.city_key = f.city_key
group by f.date_key, c.city_name, c.region
