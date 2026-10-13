select
    date_key,
    count(*)                                                        as orders_placed,
    count(*) filter (where order_status = 'cancelled')              as orders_cancelled,
    round(100.0 * count(*) filter (where order_status = 'cancelled') / nullif(count(*), 0), 2)
                                                                    as cancel_rate_pct,
    count(*) filter (where payment_status = 'failed')               as orders_payment_failed,
    round(100.0 * count(*) filter (where payment_status = 'failed') / nullif(count(*), 0), 2)
                                                                    as payment_failure_rate_pct,
    sum(revenue_amount)                                             as revenue,
    round(sum(revenue_amount) / nullif(count(*) filter (where is_revenue), 0), 2)
                                                                    as avg_order_value
from {{ ref('fact_orders') }}
group by date_key
