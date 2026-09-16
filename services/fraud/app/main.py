import asyncio
import json
import os
import time
import traceback
from collections import defaultdict, deque

import psycopg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI
from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

INPUT_TOPIC = "payments.raw"
VALIDATED_TOPIC = "payments.validated"
FRAUD_TOPIC = "payments.fraud"
DLQ_TOPIC = "payments.dlq"
RETRY_MAX_ATTEMPTS = 4
RETRY_BASE_DELAY_SECONDS = 1.0
DLQ_SIMULATION_MERCHANT = os.getenv("PAYMENT_DLQ_SIMULATION_MERCHANT", "merchant-dlq-simulation")


class DlqRoutedError(RuntimeError):
    pass

app = FastAPI(title="Fraud Service")
FRAUD_CHECKS = Counter("fraud_checks_total", "Fraud checks performed")
FRAUD_FLAGS = Counter("fraud_flags_total", "Fraud flagged payments")
FRAUD_APPROVED = Counter("fraud_approved_total", "Payments approved by fraud checks")
FRAUD_REVIEW = Counter("fraud_review_total", "Payments held for fraud review")
FRAUD_REJECTED = Counter("fraud_rejected_total", "Payments rejected by fraud checks")
producer: AIOKafkaProducer | None = None
customer_amounts: defaultdict[str, list[float]] = defaultdict(list)
customer_timestamps: defaultdict[str, deque[float]] = defaultdict(deque)


def init_db() -> None:
    with psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        dbname=os.getenv("POSTGRES_DB", "paymentdb"),
        user=os.getenv("POSTGRES_USER", "payment"),
        password=os.getenv("POSTGRES_PASSWORD", "paymentpass"),
        port=5432,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS payment_ledger (
                    id SERIAL PRIMARY KEY,
                    payment_id TEXT NOT NULL UNIQUE,
                    amount NUMERIC(12,2) NOT NULL,
                    currency TEXT NOT NULL,
                    customer_id TEXT NOT NULL,
                    merchant_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    risk_score INTEGER,
                    risk_reasons JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            cursor.execute("ALTER TABLE payment_ledger ADD COLUMN IF NOT EXISTS risk_score INTEGER")
            cursor.execute("ALTER TABLE payment_ledger ADD COLUMN IF NOT EXISTS risk_reasons JSONB")
        conn.commit()


def record_fraud_status(payload: dict, status: str) -> None:
    with psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        dbname=os.getenv("POSTGRES_DB", "paymentdb"),
        user=os.getenv("POSTGRES_USER", "payment"),
        password=os.getenv("POSTGRES_PASSWORD", "paymentpass"),
        port=5432,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE payment_ledger
                SET status = %s, risk_score = %s, risk_reasons = %s
                WHERE payment_id = %s
                """,
                (status, payload.get("risk_score"), json.dumps(payload.get("risk_reasons", [])), payload["payment_id"]),
            )
            if cursor.rowcount == 0:
                cursor.execute(
                    """
                    INSERT INTO payment_ledger
                        (payment_id, amount, currency, customer_id, merchant_id, status, risk_score, risk_reasons)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        payload["payment_id"],
                        payload["amount"],
                        payload["currency"],
                        payload["customer_id"],
                        payload["merchant_id"],
                        status,
                        payload.get("risk_score"),
                        json.dumps(payload.get("risk_reasons", [])),
                    ),
                )
        conn.commit()


def fraud_assessment(customer_id: str, amount: float, currency: str, merchant_id: str, now: float) -> tuple[int, list[str]]:
    timestamps = customer_timestamps[customer_id]
    cutoff = now - 60
    while timestamps and timestamps[0] <= cutoff:
        timestamps.popleft()

    previous_amounts = customer_amounts[customer_id]
    reasons: list[str] = []
    score = 0
    if previous_amounts and amount > (sum(previous_amounts) / len(previous_amounts)) * 2.5:
        reasons.append("behavioral")
        score += 35
    if len(timestamps) >= 2:
        reasons.append("velocity")
        score += 30
    if amount > 5000 and not previous_amounts:
        reasons.append("new-user-high-amount")
        score += 70
    if amount > 1000:
        reasons.append("high-amount")
        score += 40
    if currency not in {"USD", "EUR", "GBP"}:
        reasons.append("unsupported-currency")
        score += 30
    if merchant_id.endswith("-travel") and amount > 750:
        reasons.append("travel-amount")
        score += 15

    customer_amounts[customer_id].append(amount)
    timestamps.append(now)
    return min(score, 100), reasons


def route_to_dlq(original_topic: str, payload: dict, error: Exception, attempt_count: int) -> dict:
    return {
        "payment_id": payload.get("payment_id"),
        "source_topic": original_topic,
        "status": "dlq",
        "attempt_count": attempt_count,
        "original_payload": payload,
        "error": {
            "type": type(error).__name__,
            "message": str(error),
            "stacktrace": "\n".join(traceback.format_exception(type(error), error, error.__traceback__)),
        },
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


async def process_with_retry(
    operation,
    *,
    producer: AIOKafkaProducer | None,
    original_topic: str,
    payload: dict,
    max_retries: int = RETRY_MAX_ATTEMPTS,
    base_delay: float = RETRY_BASE_DELAY_SECONDS,
):
    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            return await operation()
        except Exception as exc:  # pragma: no cover - covered by integration path
            last_error = exc
            if attempt >= max_retries:
                dlq_message = route_to_dlq(original_topic, payload, exc, attempt)
                if producer is not None:
                    await producer.send_and_wait(
                        DLQ_TOPIC,
                        key=str(payload.get("payment_id", "unknown")).encode("utf-8"),
                        value=json.dumps(dlq_message).encode("utf-8"),
                    )
                    print(
                        f"[fraud] Routed payment {payload.get('payment_id')} to {DLQ_TOPIC} "
                        f"after {attempt} attempts"
                    )
                raise DlqRoutedError(
                    f"Payment {payload.get('payment_id')} routed to {DLQ_TOPIC}"
                ) from exc
            delay = base_delay * (2 ** (attempt - 1))
            if delay > 0:
                await asyncio.sleep(delay)
    if last_error is not None:
        raise last_error
    raise RuntimeError("retry loop exhausted without failure")


async def process_payment(payload: dict) -> None:
    if payload.get("merchant_id") == DLQ_SIMULATION_MERCHANT:
        raise RuntimeError("simulated downstream failure for DLQ demonstration")

    FRAUD_CHECKS.inc()
    amount = float(payload.get("amount", 0))
    transaction_key = payload["payment_id"].encode("utf-8")
    if "risk_score" in payload and "risk_reasons" in payload:
        score = int(payload["risk_score"])
        reasons = list(payload["risk_reasons"])
    else:
        score, reasons = fraud_assessment(
            payload["customer_id"],
            amount,
            payload.get("currency", ""),
            payload.get("merchant_id", ""),
            time.monotonic(),
        )
        payload["risk_score"] = score
        payload["risk_reasons"] = reasons
    if score >= 70:
        FRAUD_FLAGS.inc()
        FRAUD_REJECTED.inc()
        payload["status"] = "rejected"
        record_fraud_status(payload, "rejected")
        await producer.send_and_wait(
            FRAUD_TOPIC,
            key=transaction_key,
            value=json.dumps(payload).encode("utf-8"),
        )
        print(
            f"[fraud] Rejected payment {payload.get('payment_id')} with risk score {score} "
            f"({', '.join(reasons)})"
        )
        return
    if score >= 40:
        FRAUD_FLAGS.inc()
        FRAUD_REVIEW.inc()
        payload["status"] = "review"
        record_fraud_status(payload, "review")
        await producer.send_and_wait(
            FRAUD_TOPIC,
            key=transaction_key,
            value=json.dumps(payload).encode("utf-8"),
        )
        print(f"[fraud] Held payment {payload.get('payment_id')} for review with risk score {score}")
        return

    FRAUD_APPROVED.inc()
    payload["status"] = "validated"
    await producer.send_and_wait(
        VALIDATED_TOPIC,
        key=transaction_key,
        value=json.dumps(payload).encode("utf-8"),
    )
    print(f"[fraud] Approved payment {payload.get('payment_id')}")


async def consume() -> None:
    consumer = AIOKafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id="fraud-group",
    )
    await consumer.start()
    try:
        async for msg in consumer:
            payload = {}
            try:
                payload = json.loads(msg.value.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
                await consumer.commit()
                print(f"[fraud] Dropped malformed event: {exc}")
                continue
            try:
                await process_with_retry(
                    lambda payload=payload: process_payment(payload),
                    producer=producer,
                    original_topic=INPUT_TOPIC,
                    payload=payload,
                )
                await consumer.commit()
            except DlqRoutedError as exc:
                await consumer.commit()
                print(f"[fraud] {exc}")
            except Exception as exc:
                print(f"[fraud] Failed to process message for payment {payload.get('payment_id')}: {exc}")
    finally:
        await consumer.stop()


@app.on_event("startup")
async def startup_event() -> None:
    global producer
    init_db()
    producer = AIOKafkaProducer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    await producer.start()
    asyncio.create_task(consume())


@app.on_event("shutdown")
async def shutdown_event() -> None:
    if producer is not None:
        await producer.stop()


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
