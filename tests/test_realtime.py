"""Streaming window / dedup / watermark logic, run as a real Structured Streaming query
(file source stands in for Kafka; memory sink stands in for Postgres)."""
import json
import shutil

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("psycopg2")
if shutil.which("java") is None:
    pytest.skip("Java not installed", allow_module_level=True)

from pyspark.sql import SparkSession, functions as F  # noqa: E402

from quality import ORDER_SCHEMA, PAYMENT_SCHEMA  # noqa: E402
from realtime_kpis import orders_kpis, payments_kpis  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("rt-tests")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def run_stream(spark, tmp_path, rows, schema, build, name):
    src = tmp_path / f"{name}_in"
    src.mkdir()
    (src / "batch.json").write_text("\n".join(json.dumps(r) for r in rows))
    df = (spark.readStream.schema(schema).json(str(src))
          .withColumn("event_ts", F.to_timestamp("event_ts")))
    q = (build(df).writeStream.format("memory").queryName(name).outputMode("update")
         .option("checkpointLocation", str(tmp_path / f"{name}_ckpt"))
         .trigger(availableNow=True).start())
    q.awaitTermination(60)
    return spark.table(name)


def order(i, ts, status="placed", amount=100.0, eid=None, user="U1"):
    return {"event_id": eid or f"e{i}", "event_ts": ts, "order_id": f"O{i}", "user_id": user,
            "city": "Rabat", "status": status, "total_amount": amount, "currency": "MAD",
            "items": [{"product_id": "P1", "quantity": 1, "unit_price": amount}]}


def test_orders_per_minute_dedups_and_filters(spark, tmp_path):
    rows = [
        order(1, "2026-10-12T10:00:10Z", amount=100.0),
        order(2, "2026-10-12T10:00:50Z", amount=50.0),
        order(2, "2026-10-12T10:00:50Z", amount=50.0, eid="e2"),   # duplicate event_id -> ignored
        order(3, "2026-10-12T10:01:05Z", amount=70.0),
        order(4, "2026-10-12T10:01:10Z", status="cancelled"),     # not a placed order
        order(5, "2026-10-12T10:01:20Z", amount=-9.0),            # invalid amount
        order(6, "2026-10-12T10:01:30Z", user=None),              # missing user
    ]
    out = run_stream(spark, tmp_path, rows, ORDER_SCHEMA, orders_kpis, "orders_kpi")
    got = {r["window"]["start"].strftime("%H:%M"): (r["orders"], float(r["revenue"])) for r in out.collect()}
    assert got == {"10:00": (2, 150.0), "10:01": (1, 70.0)}


def test_payment_failure_rate_per_minute(spark, tmp_path):
    def pay(i, ts, status):
        return {"event_id": f"p{i}", "event_ts": ts, "payment_id": f"PAY{i}", "order_id": f"O{i}",
                "user_id": "U1", "amount": 10.0, "method": "card", "status": status}
    rows = [pay(1, "2026-10-12T10:00:01Z", "failed"), pay(2, "2026-10-12T10:00:02Z", "failed"),
            pay(3, "2026-10-12T10:00:03Z", "success"), pay(4, "2026-10-12T10:00:04Z", "success"),
            pay(5, "2026-10-12T10:00:05Z", "refunded")]            # invalid status ignored
    out = run_stream(spark, tmp_path, rows, PAYMENT_SCHEMA, payments_kpis, "pay_kpi")
    r = out.collect()[0]
    assert (r["payments"], r["failed"], r["failure_rate"]) == (4, 2, 0.5)
