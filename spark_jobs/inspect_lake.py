"""Print row counts and integrity checks for every lake layer. Used in the demos.

    docker compose run --rm --no-deps bronze-stream python inspect_lake.py
"""
from delta.tables import DeltaTable
from pyspark.sql import functions as F

from common import BRONZE, QUARANTINE, SILVER, get_spark


def main() -> None:
    spark = get_spark("inspect_lake")
    spark.sparkContext.setLogLevel("ERROR")

    def read(path):
        return spark.read.format("delta").load(path) if DeltaTable.isDeltaTable(spark, path) else None

    bronze = read(BRONZE)
    print("\n=== BRONZE (raw Kafka records) ===")
    if bronze is not None:
        bronze.groupBy("topic").count().orderBy("topic").show()

    print("=== SILVER (validated + deduplicated) ===")
    for topic, path in SILVER.items():
        df = read(path)
        if df is None:
            print(f"{topic}: (not created yet)")
            continue
        total, distinct = df.count(), df.select("event_id").distinct().count()
        flag = "OK - no duplicates" if total == distinct else "!! DUPLICATES FOUND"
        print(f"{topic:9s} rows={total:<8d} distinct_event_ids={distinct:<8d} {flag}")

    print("\n=== QUARANTINE (rejected rows by reason) ===")
    q = read(QUARANTINE)
    if q is not None:
        (q.select("topic", F.explode("reasons").alias("reason"))
          .groupBy("topic", "reason").count().orderBy(F.desc("count")).show(truncate=False))
    spark.stop()


if __name__ == "__main__":
    main()
