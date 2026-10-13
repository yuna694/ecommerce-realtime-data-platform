"""Pure transformation logic for the Gold layer (no I/O, unit-testable)."""
from typing import Dict

from pyspark.sql import DataFrame, Window, functions as F


def build_gold(orders: DataFrame, payments: DataFrame, quarantine: DataFrame) -> Dict[str, DataFrame]:
    """Collapse Silver *events* into Gold *current-state* tables.

    orders      Silver orders  (event_ts, ingested_at, order_id, status, items, ...)
    payments    Silver payments
    quarantine  Quarantine table (topic, reasons, quarantined_at)
    """
    # ---- orders_current: latest event wins; placed_at = first event; cancelled_at = cancel event
    latest = Window.partitionBy("order_id").orderBy(F.col("event_ts").desc(), F.col("ingested_at").desc())
    lifecycle = orders.groupBy("order_id").agg(
        F.min("event_ts").alias("placed_at"),
        F.max(F.when(F.col("status") == "cancelled", F.col("event_ts"))).alias("cancelled_at"),
    )
    orders_current = (orders.withColumn("rn", F.row_number().over(latest)).filter("rn = 1")
                      .join(lifecycle, "order_id")
                      .select("order_id", "user_id", "city",
                              F.col("status").alias("order_status"),
                              "total_amount", "currency", "placed_at", "cancelled_at",
                              F.to_date("placed_at").alias("order_date"), "items"))

    # ---- order_items: one row per order line
    order_items = (orders_current
                   .select("order_id", "order_date", "city", F.explode("items").alias("item"))
                   .select("order_id", "order_date", "city",
                           F.col("item.product_id").alias("product_id"),
                           F.col("item.quantity").alias("quantity"),
                           F.col("item.unit_price").alias("unit_price"))
                   .withColumn("line_amount", F.col("quantity") * F.col("unit_price")))

    # ---- payments_current: latest attempt wins, plus the number of attempts
    pw = Window.partitionBy("order_id").orderBy(F.col("event_ts").desc(), F.col("ingested_at").desc())
    attempts = payments.groupBy("order_id").agg(F.count("*").alias("payment_attempts"))
    payments_current = (payments.withColumn("rn", F.row_number().over(pw)).filter("rn = 1")
                        .join(attempts, "order_id")
                        .select("order_id",
                                F.col("status").alias("payment_status"),
                                F.col("amount").alias("payment_amount"),
                                F.col("method").alias("payment_method"),
                                "payment_attempts",
                                F.col("event_ts").alias("last_payment_at")))

    # ---- data-quality report: rejected rows per topic / reason / day
    quarantine_summary = (quarantine
                          .select("topic", F.to_date("quarantined_at").alias("quarantine_date"),
                                  F.explode("reasons").alias("reason"))
                          .groupBy("topic", "quarantine_date", "reason")
                          .agg(F.count("*").alias("rows")))

    return {
        "orders_current": orders_current,
        "order_items": order_items,
        "payments_current": payments_current,
        "quarantine_summary": quarantine_summary,
    }
