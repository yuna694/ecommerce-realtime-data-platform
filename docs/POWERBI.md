# Power BI guide

Power BI Desktop runs on Windows, so connect it straight to the Postgres container.

## 1. Connect
*Get data -> PostgreSQL database*: server `localhost:5432`, database `warehouse`, user `de`, password `de_password`.
(If Power BI asks for the Npgsql provider, install Npgsql 4.0.x from its GitHub releases, then restart Power BI.)

## 2. Tables and storage mode

| Table | Mode | Why |
|---|---|---|
| `analytics.fact_orders`, `fact_order_items`, `dim_product`, `dim_city`, `dim_customer`, `dim_date` | Import | Small, refreshed by Airflow every 10 min |
| `analytics.kpi_daily_summary`, `kpi_top_products`, `kpi_revenue_by_city` | Import | Pre-aggregated |
| `realtime.orders_per_minute`, `realtime.payments_per_minute`, `monitoring.alerts` | **DirectQuery** | Needs sub-minute freshness |
| `lake_gold.quarantine_summary`, `monitoring.pipeline_metrics` | Import | Data-quality page |

Mixing Import and DirectQuery makes a *composite model*; that is expected. For the real-time page turn on **Page refresh** (Format page -> Page refresh -> Auto, 10 seconds).

## 3. Relationships (star schema, all many-to-one, single direction)
- `fact_orders[city_key]` -> `dim_city[city_key]`
- `fact_orders[date_key]` -> `dim_date[date_key]`
- `fact_orders[customer_id]` -> `dim_customer[customer_id]`
- `fact_order_items[product_id]` -> `dim_product[product_id]`
- `fact_order_items[date_key]` -> `dim_date[date_key]`
- `fact_order_items[city_key]` -> `dim_city[city_key]`

Keep the `realtime` and `monitoring` tables unrelated (standalone).

## 4. DAX measures
```DAX
Revenue = SUM ( fact_orders[revenue_amount] )
Orders = COUNTROWS ( fact_orders )
Paid Orders = CALCULATE ( COUNTROWS ( fact_orders ), fact_orders[is_revenue] = TRUE () )
Avg Order Value = DIVIDE ( [Revenue], [Paid Orders] )
Cancel Rate = DIVIDE ( CALCULATE ( COUNTROWS ( fact_orders ), fact_orders[order_status] = "cancelled" ), [Orders] )
Payment Failure Rate = DIVIDE ( CALCULATE ( COUNTROWS ( fact_orders ), fact_orders[payment_status] = "failed" ), [Orders] )
Units Sold = CALCULATE ( SUM ( fact_order_items[quantity] ), fact_order_items[is_revenue] = TRUE () )
Latest Failure Rate = MAXX ( TOPN ( 1, 'payments_per_minute', 'payments_per_minute'[window_start], DESC ), 'payments_per_minute'[failure_rate] )
Quarantine Rate = MAXX ( TOPN ( 1, 'pipeline_metrics', 'pipeline_metrics'[run_ts], DESC ), 'pipeline_metrics'[quarantine_rate] )
```

## 5. Pages

**Page 1 - Executive overview**: cards (Revenue, Orders, Avg Order Value, Cancel Rate, Payment Failure Rate); revenue by city (map or bar); top 10 products by units (bar, from `dim_product`); daily revenue trend (line); slicers for date, city, category.

**Page 2 - Real-time operations** (auto refresh 10 s): line chart orders per minute (`orders_per_minute`, last 30 min via relative-time filter); line chart payment failure rate with a constant line at 25 %; card "Latest Failure Rate" with conditional colour (red above 0.25); table of `monitoring.alerts` (latest 10).

**Page 3 - Data quality and pipeline health**: Quarantine Rate card; stacked bar of rejected rows by `reason` and `topic` (`quarantine_summary`); table of `pipeline_metrics` (rows per layer, last run); a note explaining each rule.

## 6. Tip for the demo video
Run the incident scenario from the README while Page 2 is open: the failure-rate line crosses the threshold, the card turns red and a new alert appears within about a minute.

> Power BI files (.pbix) are binary and can't be generated from code, so this guide is the specification: building it yourself takes ~1 hour and is part of the learning.
