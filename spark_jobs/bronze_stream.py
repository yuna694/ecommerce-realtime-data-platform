"""Bronze: land raw Kafka records in Delta, untouched.

Why raw? Bronze is the replayable source of truth. If a parsing bug or a new
business rule appears, Silver/Gold can be rebuilt from Bronze without asking
Kafka (whose retention is short) for the data again.
"""
from pyspark.sql import functions as F

from common import BRONZE, CHECKPOINTS, KAFKA_BOOTSTRAP, get_spark, log


def main() -> None:
    spark = get_spark("bronze_stream")
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", "orders,payments,clicks")
           .option("startingOffsets", "earliest")
           .option("failOnDataLoss", "false")
           .load())

    bronze = raw.select(
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.col("key").cast("string").alias("key"),
        F.col("value").cast("string").alias("value"),   # raw JSON, even if malformed
        F.current_timestamp().alias("ingested_at"),
        F.current_date().alias("ingest_date"),
    )

    query = (bronze.writeStream.format("delta")
             .outputMode("append")
             .partitionBy("ingest_date", "topic")
             .option("checkpointLocation", f"{CHECKPOINTS}/bronze_events")
             .trigger(processingTime="5 seconds")
             .start(BRONZE))
    log(f"bronze stream started -> {BRONZE}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
