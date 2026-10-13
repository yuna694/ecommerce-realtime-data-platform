# Interview guide

## The 60-second pitch

> "I built an e-commerce analytics platform that processes events in real time and in batch. A Python generator simulates users browsing, ordering and paying - including dirty data on purpose. Events go into **Kafka**; **Spark Structured Streaming** lands them raw in a **Delta Lake Bronze** layer, then validates and deduplicates them into **Silver**, sending bad rows to a quarantine table with the reason. A batch job builds **Gold** current-state tables, **Airflow** orchestrates the batch and **dbt** builds a star schema with 26 automated tests. A separate streaming job computes per-minute KPIs and raises an alert when payment failures spike. **Power BI** sits on top. It is idempotent, recovers from crashes via checkpoints, and I tested the transformation logic with pytest on real Spark."

## Why each tool exists (and what else could have been used)

| Tool | The problem it solves here | Alternatives / when I'd pick them |
|---|---|---|
| **Kafka** | Durable, replayable, ordered-per-key event log that decouples producers from consumers and absorbs spikes. | Kinesis / Pub/Sub / Event Hubs (managed, same idea); RabbitMQ is a queue - no replay. |
| **Spark Structured Streaming** | Distributed, fault-tolerant stream processing with windows, watermarks, checkpoints, and one API for batch + streaming. | Flink (lower latency, true event-at-a-time); Kafka Streams (JVM, no cluster). |
| **Delta Lake** | ACID tables on files: `MERGE` for dedup, schema enforcement, time travel, streaming source *and* sink. | Iceberg / Hudi (same category); plain Parquet has no transactions or upserts. |
| **Bronze/Silver/Gold** | Separate raw (replayable) from clean (trusted) from business-ready, so each layer has one job and bugs are fixable by replay. | - |
| **Airflow** | Scheduling, dependencies, retries, backfills, visibility for *batch* tasks. | Dagster / Prefect / cloud schedulers. Not for streaming - streams are always-on services. |
| **dbt** | SQL transformations as versioned code with tests, docs and lineage; analytics engineers can own it. | Spark SQL (fine, but no built-in test/doc framework); stored procedures (no versioning/tests). |
| **Postgres** | Serving layer for BI. | Synapse / Snowflake / BigQuery / Databricks SQL at scale. |
| **Power BI** | Business-facing dashboards. | Tableau, Superset, Looker. |

## Questions you should be ready for

**1. What delivery guarantees do you have?**
Kafka -> Spark is at-least-once. I make it effectively-once at the sink: Silver uses `MERGE ... WHEN NOT MATCHED INSERT` on `event_id`, so replays and producer duplicates do nothing; the quarantine append uses Delta's `txnAppId/txnVersion` so a replayed micro-batch isn't written twice. The producer uses `enable.idempotence` and `acks=all`.

**2. What happens if Spark crashes mid-batch?**
The checkpoint stores the processed offsets / Delta versions. On restart it replays the incomplete micro-batch (`foreachBatch` is at-least-once), and idempotent writes make that harmless. I demo this by killing the container.

**3. What is a watermark and what happens to late events?**
It's how long the stream waits for out-of-order events before finalising a window and discarding old state (2 minutes here). Events later than that are dropped *from the speed layer only*. They are still in Bronze/Silver, so the batch layer includes them - the speed layer is fast but approximate, the batch layer is accurate.

**4. Why did you keep a raw Bronze layer?**
If a parser bug or new business rule appears, I can rebuild Silver/Gold from Bronze. Kafka retention is short, so Bronze is the long-term replayable record. Bronze never fails on bad data because it stores the raw string.

**5. How do you do data quality?**
Row-level rules in `quality.py` (nulls, non-positive amounts, invalid enums, malformed JSON). Failing rows go to quarantine with the raw payload and list of reasons - nothing is silently dropped. A summary table feeds the dashboard. Then dbt tests check the warehouse: uniqueness, referential integrity, accepted values, order total = sum of lines, no revenue on unpaid orders. I verified the tests fail by injecting a negative amount and an unknown city.

**6. Why `DECIMAL` for money?**
Floating point can't represent most decimal fractions exactly, so sums drift. `DECIMAL(12,2)` is exact.

**7. How is ordering handled in Kafka?**
Ordering is only guaranteed within a partition. I key `orders` and `payments` by `order_id` so all events of one order land in the same partition in order. Clicks are keyed by `user_id`.

**8. Why does Gold collapse events into "current state"?**
An order produces several events (placed, cancelled); a payment can fail then succeed on retry. BI users want one row per order with its latest status. Gold uses `row_number()` over (order by event time desc) to pick the latest, and keeps `placed_at`, `cancelled_at`, `payment_attempts`.

**9. Why both a streaming job and Airflow?**
They do different jobs. Streaming services run continuously for low latency. Airflow runs scheduled, dependent steps (Gold -> dbt seed -> run -> test), with retries and failure alerts. Using Airflow to "run streaming" would be a misuse.

**10. How does your pipeline handle schema changes?**
Events carry `schema_version`. Bronze stores raw values so nothing breaks upstream; Silver parsing is explicit, so a new field is added deliberately (and Delta supports schema evolution). Next step would be a Schema Registry with Avro and compatibility rules.

**11. How would you scale it?**
More Kafka partitions and brokers (replication >= 3); run Spark on a cluster (Databricks / K8s / YARN) and scale executors; partition Delta by date with `OPTIMIZE`/compaction; prune the `MERGE` by partition; move serving to a cloud warehouse; run Airflow with Celery/K8s executors.

**12. What is a star schema and what's the grain?**
Facts surrounded by dimensions. `fact_orders` has one row per order; `fact_order_items` one row per order line. Dimensions: customer, product, city, date. Declaring the grain first prevents double-counting.

**13. Lambda vs Kappa?**
This is Lambda-style: a speed layer plus a batch layer. Kappa uses one streaming path and reprocesses by replay. With Delta + Spark's unified API I could simplify toward Kappa; I kept both to show the trade-off between freshness and correctness.

**14. How do you monitor it?**
Airflow task states and retries; `monitoring.pipeline_metrics` (row counts per layer, quarantine rate); `monitoring.alerts` (payment failure spike, task failures); dbt source freshness. Next: Prometheus/Grafana and alert routing to Slack/email.

**15. What would you do differently in production?**
Managed Kafka, real cluster, Schema Registry, secrets management, CI/CD with the full stack, Great Expectations or Soda, lineage (OpenLineage), cost/retention policies, and PII handling for user ids.

## Honest limitations (say them before they ask)

Local single-JVM Spark, single Kafka broker, truncate+insert load to Postgres, generator emits a session's events at once. Naming these shows you understand what production would change.
