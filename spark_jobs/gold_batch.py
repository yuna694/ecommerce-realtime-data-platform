"""Gold batch job: Silver (Delta) -> Gold (Delta) -> Postgres warehouse.

The transformation logic lives in gold_logic.py (pure, unit-tested); this file is I/O.
Orchestrated by Airflow. Safe to re-run: every write is a full overwrite (idempotent).
"""
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, functions as F

from common import BRONZE, GOLD, JDBC_PROPS, JDBC_URL, QUARANTINE, SILVER, get_spark, log
from gold_logic import build_gold


def load_to_postgres(df: DataFrame, table: str, mode: str = "overwrite") -> None:
    # truncate=true keeps the table (and the dbt views built on it) and only replaces rows
    df.write.jdbc(JDBC_URL, table, mode=mode, properties={**JDBC_PROPS, "truncate": "true"})
    log(f"loaded {table} ({mode})")


def main() -> None:
    spark = get_spark("gold_batch")
    for path in (*SILVER.values(), QUARANTINE):
        if not DeltaTable.isDeltaTable(spark, path):
            raise RuntimeError(f"Silver table missing: {path} (is the silver stream running?)")

    orders = spark.read.format("delta").load(SILVER["orders"])
    payments = spark.read.format("delta").load(SILVER["payments"])
    clicks = spark.read.format("delta").load(SILVER["clicks"])
    quarantine = spark.read.format("delta").load(QUARANTINE)

    gold = build_gold(orders, payments, quarantine)
    for name, df in gold.items():
        path = f"{GOLD}/{name}"
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
        gold[name] = spark.read.format("delta").load(path)   # read back what was committed

    # ---- run-level metrics (monitoring)
    n_bronze = spark.read.format("delta").load(BRONZE).count()
    n_quarantine = quarantine.count()
    metrics = spark.createDataFrame([(
        n_bronze, clicks.count(), orders.count(), payments.count(),
        n_quarantine, n_quarantine / max(n_bronze, 1),
    )], "bronze_rows long, silver_clicks long, silver_orders long, silver_payments long, "
        "quarantine_rows long, quarantine_rate double").withColumn("run_ts", F.current_timestamp())
    log(f"bronze={n_bronze} quarantine={n_quarantine}")

    # ---- publish to the warehouse: schema `lake_gold` is dbt's source layer
    load_to_postgres(gold["orders_current"].drop("items"), "lake_gold.orders_current")
    load_to_postgres(gold["order_items"], "lake_gold.order_items")
    load_to_postgres(gold["payments_current"], "lake_gold.payments_current")
    load_to_postgres(gold["quarantine_summary"], "lake_gold.quarantine_summary")
    load_to_postgres(metrics, "monitoring.pipeline_metrics", mode="append")
    spark.stop()


if __name__ == "__main__":
    main()
