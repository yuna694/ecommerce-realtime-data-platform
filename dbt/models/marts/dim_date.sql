with bounds as (
    select
        coalesce(min(order_date), current_date)                       as d0,
        greatest(coalesce(max(order_date), current_date), current_date) as d1
    from {{ ref('stg_orders') }}
)
select
    d::date                                   as date_key,
    extract(year  from d)::int                as year,
    extract(month from d)::int                as month,
    to_char(d, 'Mon')                         as month_name,
    extract(isodow from d)::int               as iso_day_of_week,
    to_char(d, 'Dy')                          as day_name,
    extract(isodow from d) in (6, 7)          as is_weekend
from bounds, generate_series(bounds.d0, bounds.d1, interval '1 day') as d
