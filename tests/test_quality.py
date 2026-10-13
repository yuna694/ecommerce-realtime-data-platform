"""Runs the Bronze->Silver validation rules on a real (local) SparkSession."""
import json
import random
import shutil
from datetime import datetime, timezone

import pytest

pytest.importorskip("pyspark")
if shutil.which("java") is None:
    pytest.skip("Java not installed", allow_module_level=True)

from pyspark.sql import SparkSession, functions as F  # noqa: E402

from events import Config, EventFactory  # noqa: E402
from quality import split, validate  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("quality-tests")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def bronze(spark, topic, values):
    rows = [(topic, v, datetime.now(timezone.utc)) for v in values]
    return spark.createDataFrame(rows, "topic string, value string, ingested_at timestamp")


def order(**kw):
    base = {"event_id": "e1", "event_ts": "2026-10-12T10:00:00.123Z", "order_id": "O1", "user_id": "U1",
            "city": "Rabat", "status": "placed", "total_amount": 100.0, "currency": "MAD",
            "items": [{"product_id": "P001", "quantity": 1, "unit_price": 100.0}], "schema_version": 1}
    base.update(kw)
    return json.dumps(base)


def reasons_of(spark, value, topic="orders"):
    v = validate(bronze(spark, topic, [value]), topic)
    return v.select("reasons").first()["reasons"]


def test_valid_order_has_no_reasons(spark):
    assert reasons_of(spark, order()) == []


@pytest.mark.parametrize("override, expected", [
    ({"user_id": None}, "missing_user_id"),
    ({"total_amount": -5.0}, "non_positive_amount"),
    ({"total_amount": 0}, "non_positive_amount"),
    ({"status": "teleported"}, "invalid_status"),
    ({"items": []}, "empty_items"),
    ({"event_ts": "not-a-date"}, "invalid_event_ts"),
    ({"event_id": None}, "missing_event_id"),
    ({"city": None}, "missing_city"),
])
def test_each_rule_fires(spark, override, expected):
    assert expected in reasons_of(spark, order(**override))


def test_malformed_json_is_flagged_not_crashing(spark):
    assert reasons_of(spark, order()[:40]) == ["malformed_json"]


def test_payment_and_click_rules(spark):
    pay = json.dumps({"event_id": "p1", "event_ts": "2026-10-12T10:00:00Z", "payment_id": "PAY1",
                      "order_id": "O1", "user_id": "U1", "amount": -1.0, "method": "card",
                      "status": "refunded", "schema_version": 1})
    assert set(reasons_of(spark, pay, "payments")) == {"non_positive_amount", "invalid_status"}
    click = json.dumps({"event_id": "c1", "event_ts": "2026-10-12T10:00:00Z", "user_id": "U1",
                        "session_id": "s", "product_id": None, "action": "view", "city": "Rabat",
                        "device": "mobile", "schema_version": 1})
    assert reasons_of(spark, click, "clicks") == ["missing_product_id"]


def test_split_dedups_within_batch_and_quarantines_bad_rows(spark):
    good, dup = order(), order()
    bad = order(event_id="e2", total_amount=-1.0)
    valid, invalid = split(validate(bronze(spark, "orders", [good, dup, bad]), "orders"), "orders")
    assert valid.count() == 1
    assert invalid.count() == 1
    row = invalid.first()
    assert row["reasons"] == ["non_positive_amount"] and "e2" in row["raw_value"]
    v = valid.first()
    assert v["event_date"].isoformat() == "2026-10-12" and v["is_late"] is True  # ingested "now", event in the past


def test_end_to_end_with_dirty_generator_output(spark):
    """Everything the generator can emit must either pass validation or be quarantined - no crashes,
    and the injected defects must be caught."""
    f = EventFactory(random.Random(11), Config(dup_rate=0.1, null_user_rate=0.1, negative_amount_rate=0.1,
                                               late_rate=0.1, malformed_rate=0.1))
    msgs = f.batch(300)
    total_invalid = total_valid = 0
    for topic in ("orders", "payments", "clicks"):
        values = [m.value for m in msgs if m.topic == topic]
        valid, invalid = split(validate(bronze(spark, topic, values), topic), topic)
        total_valid += valid.count()
        total_invalid += invalid.count()
        reasons = {r for row in invalid.select(F.explode("reasons").alias("r")).collect() for r in [row["r"]]}
        assert reasons <= {"malformed_json", "missing_user_id", "non_positive_amount"}, reasons
        assert valid.select("event_id").distinct().count() == valid.count()   # deduplicated
    assert total_valid > 0 and total_invalid > 0
