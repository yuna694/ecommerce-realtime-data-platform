"""Speed layer: sub-minute KPIs straight from Kafka, upserted into Postgres.

Two streaming queries share one Spark app:
  * orders/minute + revenue/minute
  * payments/minute + failure rate, with an alert when the failure rate spikes

Watermark = how long we wait for late events before closing a window. Events later
than that are dropped *from this layer only*; they are still in Bronze/Silver/Gold,
so the batch path corrects the numbers (the classic speed layer / batch layer split).
"""
import os

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame, functions as F

from common import CHECKPOINTS, KAFKA_BOOTSTRAP, PG, get_spark, log
from quality import ORDER_SCHEMA, PAYMENT_SCHEMA

WATERMARK = os.getenv("WATERMARK", "2 minutes")
ALERT_FAILURE_RATE = float(os.getenv("ALERT_FAILURE_RATE", "0.25"))
ALERT_MIN_PAYMENTS = int(os.getenv("ALERT_MIN_PAYMENTS", "20"))


def read_topic(spark, topic: str, schema) -> DataFrame:
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", topic)
           .option("startingOffsets", "latest")
           .option("failOnDataLoss", "false")
           .load())
    return (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e")).select("e.*")
            .withColumn("event_ts", F.to_timestamp("event_ts")))


def pg_execute(sql: str, rows: list) -> None:
    if not rows:
        return
    conn = psycopg2.connect(**PG)
    try:
        with conn, conn.cursor() as cur:
            execute_values(cur, sql, rows)
    finally:
        conn.close()


def upsert_orders(batch: DataFrame, batch_id: int) -> None:
    rows = [(r["start"], r["orders"], r["revenue"]) for r in
            batch.select(F.col("window.start").alias("start"), "orders", "revenue").collect()]
    pg_execute("""
        INSERT INTO realtime.orders_per_minute (window_start, orders, revenue) VALUES %s
        ON CONFLICT (window_start) DO UPDATE
          SET orders = EXCLUDED.orders, revenue = EXCLUDED.revenue, updated_at = now()""", rows)
    log(f"orders batch {batch_id}: upserted {len(rows)} windows")


def upsert_payments(batch: DataFrame, batch_id: int) -> None:
    rows = [(r["start"], r["payments"], r["failed"], float(r["failure_rate"])) for r in
            batch.select(F.col("window.start").alias("start"), "payments", "failed", "failure_rate").collect()]
    pg_execute("""
        INSERT INTO realtime.payments_per_minute (window_start, payments, failed, failure_rate) VALUES %s
        ON CONFLICT (window_start) DO UPDATE
          SET payments = EXCLUDED.payments, failed = EXCLUDED.failed,
              failure_rate = EXCLUDED.failure_rate, updated_at = now()""", rows)

    alerts = [(
        "payments_failure_spike", "critical", r[0],
        f"Payment failure rate {r[3]:.0%} ({r[2]}/{r[1]}) in minute starting {r[0]:%H:%M}",
    ) for r in rows if r[1] >= ALERT_MIN_PAYMENTS and r[3] > ALERT_FAILURE_RATE]
    pg_execute("""
        INSERT INTO monitoring.alerts (source, severity, window_start, message) VALUES %s
        ON CONFLICT (source, window_start) DO NOTHING""", alerts)
    if alerts:
        log(f"ALERT: {alerts[-1][3]}")


def orders_kpis(orders: DataFrame, watermark: str = WATERMARK) -> DataFrame:
    """Orders + revenue per 1-minute event-time window (placed orders only, deduplicated)."""
    return (orders
            .filter(F.col("event_id").isNotNull() & F.col("event_ts").isNotNull()
                    & F.col("user_id").isNotNull() & (F.col("status") == "placed")
                    & (F.col("total_amount") > 0))
            .withWatermark("event_ts", watermark)
            .dropDuplicates(["event_id"])
            .groupBy(F.window("event_ts", "1 minute"))
            .agg(F.count("*").alias("orders"), F.sum("total_amount").alias("revenue")))


def payments_kpis(payments: DataFrame, watermark: str = WATERMARK) -> DataFrame:
    """Payments, failures and failure rate per 1-minute event-time window."""
    return (payments
            .filter(F.col("event_id").isNotNull() & F.col("event_ts").isNotNull()
                    & F.col("status").isin("success", "failed") & (F.col("amount") > 0))
            .withWatermark("event_ts", watermark)
            .dropDuplicates(["event_id"])
            .groupBy(F.window("event_ts", "1 minute"))
            .agg(F.count("*").alias("payments"),
                 F.sum(F.when(F.col("status") == "failed", 1).otherwise(0)).alias("failed"))
            .withColumn("failure_rate", F.col("failed") / F.col("payments")))


def main() -> None:
    spark = get_spark("realtime_kpis")
    queries = (
        ("orders", orders_kpis(read_topic(spark, "orders", ORDER_SCHEMA)), upsert_orders),
        ("payments", payments_kpis(read_topic(spark, "payments", PAYMENT_SCHEMA)), upsert_payments),
    )
    for name, df, fn in queries:
        (df.writeStream.outputMode("update").foreachBatch(fn)
         .option("checkpointLocation", f"{CHECKPOINTS}/realtime_{name}")
         .trigger(processingTime="10 seconds").start())
    log("realtime KPI streams started")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
