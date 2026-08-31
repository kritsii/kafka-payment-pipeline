import asyncio
import json
import os
import time
import traceback
from collections import defaultdict, deque

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

app = FastAPI(title="Fraud Service")
FRAUD_CHECKS = Counter("fraud_checks_total", "Fraud checks performed")
FRAUD_FLAGS = Counter("fraud_flags_total", "Fraud flagged payments")
producer: AIOKafkaProducer | None = None
customer_amounts: defaultdict[str, list[float]] = defaultdict(list)
customer_timestamps: defaultdict[str, deque[float]] = defaultdict(deque)


def fraud_reasons(customer_id: str, amount: float, now: float) -> list[str]:
    timestamps = customer_timestamps[customer_id]
    cutoff = now - 60
    while timestamps and timestamps[0] <= cutoff:
        timestamps.popleft()

    previous_amounts = customer_amounts[customer_id]
    reasons: list[str] = []
    if previous_amounts and amount > (sum(previous_amounts) / len(previous_amounts)) * 2.5:
        reasons.append("behavioral")
    if len(timestamps) >= 2:
        reasons.append("velocity")
    if amount > 5000 and not previous_amounts:
        reasons.append("new-user-high-amount")

    customer_amounts[customer_id].append(amount)
    timestamps.append(now)
    return reasons


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
                raise
            delay = base_delay * (2 ** (attempt - 1))
            if delay > 0:
                await asyncio.sleep(delay)
    if last_error is not None:
        raise last_error
    raise RuntimeError("retry loop exhausted without failure")


async def process_payment(payload: dict) -> None:
    FRAUD_CHECKS.inc()
    amount = float(payload.get("amount", 0))
    transaction_key = payload["payment_id"].encode("utf-8")
    reasons = fraud_reasons(payload["customer_id"], amount, time.monotonic())
    if reasons:
        FRAUD_FLAGS.inc()
        payload["status"] = "fraud"
        await producer.send_and_wait(
            FRAUD_TOPIC,
            key=transaction_key,
            value=json.dumps(payload).encode("utf-8"),
        )
        print(
            f"[fraud] Flagged payment {payload.get('payment_id')} as suspicious "
            f"({', '.join(reasons)})"
        )
        return

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
        enable_auto_commit=True,
        group_id="fraud-group",
    )
    await consumer.start()
    try:
        async for msg in consumer:
            payload = json.loads(msg.value.decode("utf-8"))
            try:
                await process_with_retry(
                    lambda payload=payload: process_payment(payload),
                    producer=producer,
                    original_topic=INPUT_TOPIC,
                    payload=payload,
                )
            except Exception as exc:
                print(f"[fraud] Failed to process message for payment {payload.get('payment_id')}: {exc}")
    finally:
        await consumer.stop()


@app.on_event("startup")
async def startup_event() -> None:
    global producer
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
