"""Synthetic credit-risk application generator -> Kafka topic.

The label `default` depends on the features (+ noise) so that a model can really learn something.
A small share of dirty rows (null income, duplicates) is injected to give the silver layer something to clean.
"""
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "credit_applications")
RATE = float(os.getenv("EVENTS_PER_SEC", "5"))
MAX_MB = float(os.getenv("MAX_DATA_MB", "60"))
STATE_FILE = os.getenv("GENERATOR_STATE", "/state/sent_bytes")


def connect():
    for i in range(30):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode(),
                key_serializer=lambda k: k.encode(),
                acks=1, linger_ms=50,
            )
        except NoBrokersAvailable:
            print(f"Kafka not ready, retry {i + 1}/30", flush=True)
            time.sleep(2)
    raise SystemExit("Kafka unavailable")


def make_event():
    income = max(8000.0, random.gauss(45000, 20000))
    loan = random.uniform(1000, 40000)
    score = int(min(850, max(300, random.gauss(650, 90))))
    late = max(0, int(random.expovariate(1.2)))
    dti = round(loan / income, 3)
    risk = 0.004 * (650 - score) + 0.5 * dti + 0.15 * late + random.gauss(0, 0.3)
    return {
        "application_id": str(uuid.uuid4()),
        "customer_id": random.randint(1, 5000),
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "age": random.randint(18, 75),
        "income": round(income, 2),
        "employment_years": random.randint(0, 40),
        "loan_amount": round(loan, 2),
        "loan_term_months": random.choice([12, 24, 36, 48, 60]),
        "interest_rate": round(random.uniform(3, 22), 2),
        "credit_score": score,
        "num_open_accounts": random.randint(0, 15),
        "num_late_payments": late,
        "debt_to_income": dti,
        "home_ownership": random.choice(["RENT", "OWN", "MORTGAGE"]),
        "loan_purpose": random.choice(["car", "home", "education", "business", "debt"]),
        "default": int(risk > 0.9),
    }


def load_state():
    try:
        with open(STATE_FILE) as f:
            return int(f.read().strip() or 0)
    except (OSError, ValueError):
        return 0


def save_state(n_bytes):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        f.write(str(n_bytes))


if __name__ == "__main__":
    # Stop after MAX_DATA_MB of JSON has been published. The byte counter lives in a volume, so restarting
    # the stack does not generate another batch (stop.ps1 -Purge resets it together with the data).
    sent_bytes = load_state()
    extra_mb = float(os.getenv("EXTRA_DATA_MB", "0"))
    if extra_mb > 0:
        # generate_more.ps1: simulate new applications arriving on top of what was already produced
        limit = sent_bytes + int(extra_mb * 1024 * 1024)
        MAX_MB = round(limit / 1024 / 1024, 1)
    else:
        limit = int(MAX_MB * 1024 * 1024)
    if sent_bytes >= limit:
        print(f"Already generated {sent_bytes / 1e6:.1f} MB (limit {MAX_MB} MB) - nothing to do.", flush=True)
        raise SystemExit(0)

    producer = connect()
    print(f"Producing ~{RATE} events/s to {TOPIC}, until {MAX_MB} MB "
          f"({sent_bytes / 1e6:.1f} MB already sent)", flush=True)
    sent = 0
    while sent_bytes < limit:
        e = make_event()
        if random.random() < 0.02:
            e["income"] = None                       # dirty row -> rejected by silver
        copies = 2 if random.random() < 0.01 else 1  # duplicate -> deduped by silver
        for _ in range(copies):
            producer.send(TOPIC, key=e["application_id"], value=e)
            sent_bytes += len(json.dumps(e).encode())
        sent += 1
        if sent % 500 == 0:
            save_state(sent_bytes)
            print(f"{sent} events, {sent_bytes / 1e6:.1f}/{MAX_MB} MB", flush=True)
        time.sleep(1.0 / RATE)

    producer.flush()
    save_state(sent_bytes)
    print(f"Done: {sent_bytes / 1e6:.1f} MB published. Generator stopped.", flush=True)
