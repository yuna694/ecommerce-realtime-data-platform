"""Shared config + helpers for all Spark jobs."""
import os
import time

from pyspark.sql import DataFrame, SparkSession

LAKE = os.getenv("LAKE_PATH", "/lake")
BRONZE = f"{LAKE}/bronze/events"
SILVER = {t: f"{LAKE}/silver/{t}" for t in ("clicks", "orders", "payments")}
QUARANTINE = f"{LAKE}/silver/quarantine"
GOLD = f"{LAKE}/gold"
CHECKPOINTS = f"{LAKE}/_checkpoints"

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

PG = {
    "host": os.getenv("WAREHOUSE_HOST", "warehouse"),
    "port": int(os.getenv("WAREHOUSE_PORT", "5432")),
    "dbname": os.getenv("WAREHOUSE_DB", "warehouse"),
    "user": os.getenv("WAREHOUSE_USER", "de"),
    "password": os.getenv("WAREHOUSE_PASSWORD", "de_password"),
}
JDBC_URL = f"jdbc:postgresql://{PG['host']}:{PG['port']}/{PG['dbname']}"
JDBC_PROPS = {"user": PG["user"], "password": PG["password"], "driver": "org.postgresql.Driver"}

# Versions must match the pip packages in the Dockerfiles (pyspark 3.5.3 / delta-spark 3.2.0).
PACKAGES = ",".join([
    "io.delta:delta-spark_2.12:3.2.0",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
    "org.postgresql:postgresql:42.7.3",
])


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def get_spark(app_name: str) -> SparkSession:
    spark = (SparkSession.builder.appName(app_name)
             .master(os.getenv("SPARK_MASTER", "local[2]"))
             .config("spark.jars.packages", PACKAGES)
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "1g"))
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def wait_for_delta(spark: SparkSession, path: str, timeout_s: int = 600) -> None:
    """A Delta stream source needs the table to exist; wait for an upstream job to create it."""
    from delta.tables import DeltaTable
    waited = 0
    while not DeltaTable.isDeltaTable(spark, path):
        if waited >= timeout_s:
            raise TimeoutError(f"{path} was not created within {timeout_s}s")
        log(f"waiting for upstream table {path} ...")
        time.sleep(5)
        waited += 5


def insert_only_merge(spark: SparkSession, df: DataFrame, path: str, key: str) -> None:
    """Idempotent write: insert rows whose `key` is not already in the table.

    This is what makes the pipeline safe against Kafka redeliveries, producer
    duplicates and foreachBatch replays after a crash (at-least-once -> effectively-once).
    """
    from delta.tables import DeltaTable
    if DeltaTable.isDeltaTable(spark, path):
        (DeltaTable.forPath(spark, path).alias("t")
         .merge(df.alias("s"), f"t.{key} = s.{key} AND t.event_date = s.event_date")
         .whenNotMatchedInsertAll()
         .execute())
    else:
        df.write.format("delta").partitionBy("event_date").mode("append").save(path)
