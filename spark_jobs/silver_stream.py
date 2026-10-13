"""Silver: parse, validate, deduplicate. Bad rows go to a quarantine table.

Reads Bronze as a stream (Delta is both a sink and a streaming source), then in
foreachBatch applies the rules from quality.py and writes with an idempotent MERGE.
"""
from pyspark.sql import DataFrame

from common import BRONZE, CHECKPOINTS, QUARANTINE, SILVER, get_spark, insert_only_merge, log, wait_for_delta
from quality import TOPICS, split, validate


def main() -> None:
    spark = get_spark("silver_stream")
    wait_for_delta(spark, BRONZE)

    def process_batch(batch: DataFrame, batch_id: int) -> None:
        batch.persist()
        try:
            for topic in TOPICS:
                valid, invalid = split(validate(batch, topic), topic)
                valid.persist()
                invalid.persist()
                n_valid, n_bad = valid.count(), invalid.count()
                if n_valid:
                    insert_only_merge(spark, valid, SILVER[topic], "event_id")
                if n_bad:
                    # txnAppId/txnVersion make this append idempotent if the batch is replayed.
                    (invalid.write.format("delta").mode("append")
                     .option("txnAppId", f"silver_quarantine_{topic}")
                     .option("txnVersion", batch_id)
                     .save(QUARANTINE))
                log(f"batch {batch_id} {topic}: valid={n_valid} quarantined={n_bad}")
                valid.unpersist()
                invalid.unpersist()
        finally:
            batch.unpersist()

    (spark.readStream.format("delta").option("maxFilesPerTrigger", 50).load(BRONZE)
     .writeStream.foreachBatch(process_batch)
     .option("checkpointLocation", f"{CHECKPOINTS}/silver")
     .trigger(processingTime="5 seconds")
     .start()
     .awaitTermination())


if __name__ == "__main__":
    main()
