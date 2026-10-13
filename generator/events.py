"""Synthetic e-commerce event generator.

Pure Python (no Kafka import) so it can be unit-tested. `EventFactory.session()`
simulates one user session and returns the Kafka messages it produces.

The generator deliberately injects *dirty data* so the pipeline's data-quality
layer has something real to catch: duplicates, null user ids, negative amounts,
late events and malformed JSON.
"""
from __future__ import annotations

import json
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, List, Optional

from catalog import CITIES, DEVICES, PAYMENT_METHODS, PRODUCTS

SCHEMA_VERSION = 1


@dataclass
class Config:
    payment_failure_rate: float = 0.08   # probability a payment attempt fails
    retry_rate: float = 0.5              # after a failure, user retries
    cancel_rate: float = 0.04            # successful orders later cancelled
    abandon_rate: float = 0.45           # cart abandoned before checkout
    # dirty-data injection
    dup_rate: float = 0.01
    null_user_rate: float = 0.01
    negative_amount_rate: float = 0.01
    late_rate: float = 0.02
    late_max_minutes: int = 15
    malformed_rate: float = 0.003


@dataclass(frozen=True)
class Message:
    topic: str
    key: str
    value: str  # JSON string (truncated on purpose when "malformed")


def iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class EventFactory:
    def __init__(
        self,
        rng: Optional[random.Random] = None,
        cfg: Optional[Config] = None,
        clock: Optional[Callable[[], datetime]] = None,
    ):
        self.rng = rng or random.Random()
        self.cfg = cfg or Config()
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self._cities = [c for c, _ in CITIES]
        self._city_weights = [w for _, w in CITIES]

    # ------------------------------------------------------------------ helpers
    def _uuid(self) -> str:
        return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))

    def _emit(self, topic: str, key: str, payload: dict, allow_late: bool = False) -> List[Message]:
        """Serialise a payload, possibly corrupting it. Returns 1 or 2 messages."""
        cfg, rng = self.cfg, self.rng
        payload = dict(payload)

        if payload.get("user_id") is not None and rng.random() < cfg.null_user_rate:
            payload["user_id"] = None
        if topic in ("orders", "payments") and rng.random() < cfg.negative_amount_rate:
            for f in ("total_amount", "amount"):
                if f in payload:
                    payload[f] = -abs(payload[f])
        if allow_late and rng.random() < cfg.late_rate:
            payload["event_ts"] = payload["event_ts"] - timedelta(
                minutes=rng.randint(3, cfg.late_max_minutes)
            )

        value = json.dumps({**payload, "event_ts": iso(payload["event_ts"])})
        if rng.random() < cfg.malformed_rate:
            value = value[: len(value) // 2]  # broken JSON

        msg = Message(topic, key, value)
        return [msg, msg] if rng.random() < cfg.dup_rate else [msg]

    # ------------------------------------------------------------------ session
    def session(self) -> List[Message]:
        rng, cfg = self.rng, self.cfg
        out: List[Message] = []
        now = self.clock()
        ticks = [0]

        def ts() -> datetime:
            ticks[0] += 1
            return now + timedelta(milliseconds=15 * ticks[0])

        user_id = f"U{rng.randint(1, 5000):05d}"
        session_id = self._uuid()
        city = rng.choices(self._cities, self._city_weights)[0]
        device = rng.choice(DEVICES)

        def click(action: str, product_id: str) -> None:
            out.extend(self._emit("clicks", user_id, {
                "event_id": self._uuid(), "event_ts": ts(), "user_id": user_id,
                "session_id": session_id, "product_id": product_id, "action": action,
                "city": city, "device": device, "schema_version": SCHEMA_VERSION,
            }, allow_late=True))

        viewed = rng.sample(PRODUCTS, k=rng.randint(1, 5))
        cart = []
        for p in viewed:
            click("view", p[0])
        for p in viewed:
            if rng.random() < 0.45:
                click("add_to_cart", p[0])
                cart.append(p)
        if not cart or rng.random() < cfg.abandon_rate:
            return out

        # ---- checkout
        order_id = f"O{self._uuid().replace('-', '')[:12]}"
        items = [
            {"product_id": p[0], "quantity": q, "unit_price": p[3]}
            for p in cart
            for q in [rng.choices([1, 2, 3], [0.7, 0.2, 0.1])[0]]
        ]
        total = round(sum(i["quantity"] * i["unit_price"] for i in items), 2)

        def order_event(status: str, allow_late: bool) -> None:
            out.extend(self._emit("orders", order_id, {
                "event_id": self._uuid(), "event_ts": ts(), "order_id": order_id,
                "user_id": user_id, "city": city, "status": status, "items": items,
                "total_amount": total, "currency": "MAD", "schema_version": SCHEMA_VERSION,
            }, allow_late=allow_late))

        def payment_event(status: str) -> None:
            out.extend(self._emit("payments", order_id, {
                "event_id": self._uuid(), "event_ts": ts(),
                "payment_id": f"PAY{self._uuid().replace('-', '')[:10]}",
                "order_id": order_id, "user_id": user_id, "amount": total,
                "method": rng.choice(PAYMENT_METHODS), "status": status,
                "schema_version": SCHEMA_VERSION,
            }))

        order_event("placed", allow_late=True)

        paid = rng.random() >= cfg.payment_failure_rate
        payment_event("success" if paid else "failed")
        if not paid and rng.random() < cfg.retry_rate:
            paid = rng.random() < 0.7
            payment_event("success" if paid else "failed")

        cancel = (not paid and rng.random() < 0.7) or (paid and rng.random() < cfg.cancel_rate)
        if cancel:
            order_event("cancelled", allow_late=False)
        return out

    def batch(self, n_sessions: int) -> List[Message]:
        msgs: List[Message] = []
        for _ in range(n_sessions):
            msgs.extend(self.session())
        return msgs
