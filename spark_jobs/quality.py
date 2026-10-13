"""Schemas + data-quality rules for the Bronze -> Silver step.

Everything here is plain PySpark column logic (no Delta, no Kafka) so it can be
unit-tested with a local SparkSession.

Design: every rule is a named boolean expression. A row's `reasons` column is the
array of rule names it violates. Empty array => valid row => Silver.
Non-empty => Quarantine, with the raw payload and the reasons kept for debugging.
"""
from typing import Dict, List, Tuple

from pyspark.sql import Column, DataFrame, functions as F, types as T

MONEY = T.DecimalType(12, 2)
S = T.StringType()

ITEM_SCHEMA = T.StructType([
    T.StructField("product_id", S),
    T.StructField("quantity", T.IntegerType()),
    T.StructField("unit_price", MONEY),
])


def _schema(*fields: Tuple[str, T.DataType]) -> T.StructType:
    # `_corrupt` receives the raw text when the JSON cannot be parsed.
    return T.StructType([T.StructField(n, t) for n, t in fields] + [T.StructField("_corrupt", S)])


CLICK_SCHEMA = _schema(
    ("event_id", S), ("event_ts", S), ("user_id", S), ("session_id", S),
    ("product_id", S), ("action", S), ("city", S), ("device", S),
    ("schema_version", T.IntegerType()),
)
ORDER_SCHEMA = _schema(
    ("event_id", S), ("event_ts", S), ("order_id", S), ("user_id", S), ("city", S),
    ("status", S), ("items", T.ArrayType(ITEM_SCHEMA)), ("total_amount", MONEY),
    ("currency", S), ("schema_version", T.IntegerType()),
)
PAYMENT_SCHEMA = _schema(
    ("event_id", S), ("event_ts", S), ("payment_id", S), ("order_id", S), ("user_id", S),
    ("amount", MONEY), ("method", S), ("status", S), ("schema_version", T.IntegerType()),
)

TOPICS = ["clicks", "orders", "payments"]


def _bad_enum(col: Column, allowed: List[str]) -> Column:
    return col.isNull() | ~col.isin(*allowed)


def _common(e: Column) -> Dict[str, Column]:
    return {
        "missing_event_id": e["event_id"].isNull(),
        "invalid_event_ts": F.to_timestamp(e["event_ts"]).isNull(),
    }


def click_rules(e: Column) -> Dict[str, Column]:
    return {**_common(e),
            "missing_user_id": e["user_id"].isNull(),
            "missing_product_id": e["product_id"].isNull(),
            "invalid_action": _bad_enum(e["action"], ["view", "add_to_cart", "remove_from_cart"])}


def order_rules(e: Column) -> Dict[str, Column]:
    return {**_common(e),
            "missing_order_id": e["order_id"].isNull(),
            "missing_user_id": e["user_id"].isNull(),
            "missing_city": e["city"].isNull(),
            "non_positive_amount": e["total_amount"].isNull() | (e["total_amount"] <= 0),
            "empty_items": e["items"].isNull() | (F.size(e["items"]) == 0),
            "invalid_status": _bad_enum(e["status"], ["placed", "cancelled"])}


def payment_rules(e: Column) -> Dict[str, Column]:
    return {**_common(e),
            "missing_order_id": e["order_id"].isNull(),
            "missing_user_id": e["user_id"].isNull(),
            "non_positive_amount": e["amount"].isNull() | (e["amount"] <= 0),
            "invalid_status": _bad_enum(e["status"], ["success", "failed"])}


SPEC = {
    "clicks": (CLICK_SCHEMA, click_rules,
               ["event_id", "user_id", "session_id", "product_id", "action", "city", "device"]),
    "orders": (ORDER_SCHEMA, order_rules,
               ["event_id", "order_id", "user_id", "city", "status", "items", "total_amount", "currency"]),
    "payments": (PAYMENT_SCHEMA, payment_rules,
                 ["event_id", "payment_id", "order_id", "user_id", "amount", "method", "status"]),
}

LATE_THRESHOLD_SECONDS = 300  # event older than 5 min when ingested => flagged is_late


def validate(bronze: DataFrame, topic: str) -> DataFrame:
    """Parse the Bronze `value` column for one topic and compute `reasons`.

    Expects Bronze columns: topic, value, ingested_at.
    """
    schema, rules_fn, _ = SPEC[topic]
    parsed = (bronze.filter(F.col("topic") == topic)
              .withColumn("e", F.from_json("value", schema, {"columnNameOfCorruptRecord": "_corrupt"})))
    e = F.col("e")
    checks = [F.when(F.coalesce(cond, F.lit(True)), F.lit(name)) for name, cond in rules_fn(e).items()]
    reasons = F.when(e["_corrupt"].isNotNull(), F.array(F.lit("malformed_json"))) \
               .otherwise(F.array_compact(F.array(*checks)))
    return parsed.withColumn("reasons", reasons)


def split(validated: DataFrame, topic: str) -> Tuple[DataFrame, DataFrame]:
    """Return (valid_rows_for_silver, quarantine_rows)."""
    _, _, columns = SPEC[topic]
    event_ts = F.to_timestamp(F.col("e")["event_ts"])
    valid = (validated.filter(F.size("reasons") == 0)
             .select(*[F.col("e")[c].alias(c) for c in columns],
                     event_ts.alias("event_ts"),
                     F.to_date(event_ts).alias("event_date"),
                     (F.unix_timestamp("ingested_at") - F.unix_timestamp(event_ts) > LATE_THRESHOLD_SECONDS).alias("is_late"),
                     F.col("ingested_at"),
                     F.current_timestamp().alias("processed_at"))
             .dropDuplicates(["event_id"]))   # in-batch dedup; cross-batch dedup = Delta MERGE
    invalid = (validated.filter(F.size("reasons") > 0)
               .select(F.lit(topic).alias("topic"),
                       F.col("value").alias("raw_value"),
                       F.col("reasons"),
                       F.col("ingested_at"),
                       F.current_timestamp().alias("quarantined_at")))
    return valid, invalid
