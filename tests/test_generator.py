import json
import random
from datetime import datetime, timezone

from events import Config, EventFactory

FIXED_NOW = datetime(2026, 10, 12, 12, 0, 0, tzinfo=timezone.utc)


def factory(seed=1, **overrides):
    clean = dict(dup_rate=0, null_user_rate=0, negative_amount_rate=0, late_rate=0, malformed_rate=0)
    clean.update(overrides)
    return EventFactory(random.Random(seed), Config(**clean), clock=lambda: FIXED_NOW)


def parse(msgs):
    return [(m.topic, json.loads(m.value)) for m in msgs]


def test_same_seed_gives_same_events():
    a = [m.value for m in factory(7).batch(50)]
    b = [m.value for m in factory(7).batch(50)]
    assert a == b


def test_clean_events_are_valid_json_with_expected_topics():
    events = parse(factory().batch(300))
    assert {t for t, _ in events} == {"clicks", "orders", "payments"}
    for topic, e in events:
        assert e["event_id"] and e["event_ts"].endswith("Z") and e["schema_version"] == 1
        if topic == "orders":
            assert e["status"] in ("placed", "cancelled") and e["total_amount"] > 0 and e["items"]
        if topic == "payments":
            assert e["status"] in ("success", "failed") and e["amount"] > 0


def test_order_total_equals_sum_of_items():
    for topic, e in parse(factory().batch(300)):
        if topic == "orders":
            total = sum(i["quantity"] * i["unit_price"] for i in e["items"])
            assert abs(total - e["total_amount"]) < 0.01


def test_every_order_has_a_placed_event_and_payment_matches_total():
    events = parse(factory().batch(400))
    placed = {e["order_id"]: e for t, e in events if t == "orders" and e["status"] == "placed"}
    for t, e in events:
        if t == "payments":
            assert e["order_id"] in placed
            assert e["amount"] == placed[e["order_id"]]["total_amount"]


def test_incident_mode_raises_payment_failure_rate():
    def failure_rate(rate):
        pays = [e for t, e in parse(factory(3, payment_failure_rate=rate).batch(1500)) if t == "payments"]
        return sum(p["status"] == "failed" for p in pays) / len(pays)
    assert failure_rate(0.6) > 3 * failure_rate(0.08)


def test_dirty_data_injection_actually_happens():
    msgs = factory(5, dup_rate=0.2, null_user_rate=0.2, negative_amount_rate=0.2,
                   late_rate=0.2, malformed_rate=0.2).batch(300)
    raw = [m.value for m in msgs]
    assert len(raw) != len(set(raw)), "expected duplicate messages"

    broken = 0
    parsed = []
    for v in raw:
        try:
            parsed.append(json.loads(v))
        except json.JSONDecodeError:
            broken += 1
    assert broken > 0
    assert any(e.get("user_id") is None for e in parsed)
    assert any(e.get("total_amount", 1) < 0 or e.get("amount", 1) < 0 for e in parsed)
    assert any(datetime.fromisoformat(e["event_ts"].replace("Z", "+00:00")) < FIXED_NOW for e in parsed)
