"""Kafka producer: streams simulated e-commerce sessions into orders/payments/clicks."""
import os
import random
import signal
import time

from confluent_kafka import Producer

from events import Config, EventFactory

RUNNING = True


def _stop(*_):
    global RUNNING
    RUNNING = False


def main() -> None:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    rate = float(os.getenv("SESSIONS_PER_SECOND", "3"))
    run_seconds = int(os.getenv("RUN_SECONDS", "0"))      # 0 = run forever
    incident = os.getenv("INCIDENT_MODE", "0") == "1"      # simulate payment-provider outage
    seed = os.getenv("SEED")

    cfg = Config()
    if incident:
        cfg.payment_failure_rate = 0.6
        print("!! INCIDENT MODE: payment failure rate = 60%", flush=True)

    factory = EventFactory(random.Random(int(seed)) if seed else random.Random(), cfg)
    producer = Producer({
        "bootstrap.servers": bootstrap,
        "enable.idempotence": True,   # no duplicates caused by producer retries
        "acks": "all",
        "linger.ms": 50,
    })
    stats = {"sent": 0, "errors": 0}

    def on_delivery(err, _msg):
        stats["errors" if err else "sent"] += 1
        if err:
            print(f"delivery failed: {err}", flush=True)

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    started = last_report = time.time()
    print(f"Producing to {bootstrap} at ~{rate} sessions/s", flush=True)
    while RUNNING:
        for m in factory.session():
            producer.produce(m.topic, key=m.key, value=m.value, on_delivery=on_delivery)
        producer.poll(0)
        time.sleep(factory.rng.expovariate(rate))   # Poisson arrivals

        now = time.time()
        if now - last_report >= 10:
            print(f"[generator] delivered={stats['sent']} errors={stats['errors']}", flush=True)
            last_report = now
        if run_seconds and now - started >= run_seconds:
            break

    producer.flush(10)
    print(f"[generator] stopped. delivered={stats['sent']} errors={stats['errors']}", flush=True)


if __name__ == "__main__":
    main()
