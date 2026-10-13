"""Gold-layer logic: lifecycle collapsing (placed->cancelled, failed->retry success)."""
import shutil
from datetime import datetime, timezone
from decimal import Decimal

import pytest

pytest.importorskip("pyspark")
if shutil.which("java") is None:
    pytest.skip("Java not installed", allow_module_level=True)

from pyspark.sql import SparkSession  # noqa: E402

from gold_logic import build_gold  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("gold-tests")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def ts(minute, second=0):
    return datetime(2026, 10, 12, 10, minute, second, tzinfo=timezone.utc)


ORDER_DDL = ("event_id string, event_ts timestamp, order_id string, user_id string, city string, status string, "
             "items array<struct<product_id:string,quantity:int,unit_price:decimal(12,2)>>, "
             "total_amount decimal(12,2), currency string, ingested_at timestamp")
PAY_DDL = ("event_id string, event_ts timestamp, order_id string, amount decimal(12,2), method string, "
           "status string, ingested_at timestamp")
Q_DDL = "topic string, reasons array<string>, quarantined_at timestamp"

ITEMS = [("P001", 2, Decimal("50.00")), ("P002", 1, Decimal("10.00"))]


def run(spark, orders, payments, quarantine=()):
    gold = build_gold(spark.createDataFrame(orders, ORDER_DDL),
                      spark.createDataFrame(payments, PAY_DDL),
                      spark.createDataFrame(list(quarantine), Q_DDL))
    return {k: {r["order_id"] if "order_id" in r else None: r for r in v.collect()} if k != "quarantine_summary"
            else v.collect() for k, v in gold.items()}


def test_cancelled_order_takes_latest_status_and_keeps_placed_at(spark):
    orders = [
        ("e1", ts(0), "O1", "U1", "Rabat", "placed", ITEMS, Decimal("110.00"), "MAD", ts(1)),
        ("e2", ts(5), "O1", "U1", "Rabat", "cancelled", ITEMS, Decimal("110.00"), "MAD", ts(6)),
    ]
    g = run(spark, orders, [])
    o = g["orders_current"]["O1"]
    assert o["order_status"] == "cancelled"
    assert o["placed_at"].replace(tzinfo=None) == ts(0).replace(tzinfo=None)
    assert o["cancelled_at"] is not None
    assert len(g["orders_current"]) == 1


def test_items_are_exploded_with_line_amounts(spark):
    orders = [("e1", ts(0), "O1", "U1", "Rabat", "placed", ITEMS, Decimal("110.00"), "MAD", ts(1))]
    g = run(spark, orders, [])
    lines = spark.createDataFrame(orders, ORDER_DDL)
    result = build_gold(lines, spark.createDataFrame([], PAY_DDL), spark.createDataFrame([], Q_DDL))["order_items"]
    amounts = sorted(r["line_amount"] for r in result.collect())
    assert amounts == [Decimal("10.00"), Decimal("100.00")]
    assert sum(amounts) == g["orders_current"]["O1"]["total_amount"]


def test_payment_retry_latest_attempt_wins_and_counts_attempts(spark):
    pays = [
        ("p1", ts(0, 1), "O1", Decimal("110.00"), "card", "failed", ts(1)),
        ("p2", ts(0, 30), "O1", Decimal("110.00"), "card", "success", ts(1)),
    ]
    g = run(spark, [], pays)
    p = g["payments_current"]["O1"]
    assert p["payment_status"] == "success" and p["payment_attempts"] == 2


def test_quarantine_summary_counts_by_reason(spark):
    q = [("orders", ["missing_user_id", "non_positive_amount"], ts(0)),
         ("orders", ["missing_user_id"], ts(1))]
    g = run(spark, [], [], q)
    counts = {r["reason"]: r["rows"] for r in g["quarantine_summary"]}
    assert counts == {"missing_user_id": 2, "non_positive_amount": 1}
