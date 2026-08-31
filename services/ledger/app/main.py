import asyncio
import json
import os
import time
import traceback

import psycopg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI
from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

TOPIC = "payments.validated"
SETTLED_TOPIC = "payments.settled"
DLQ_TOPIC = "payments.dlq"
RETRY_MAX_ATTEMPTS = 4
RETRY_BASE_DELAY_SECONDS = 1.0

app = FastAPI(title="Ledger Service")
LEDGER_WRITES = Counter("ledger_writes_total", "Ledger records written")
producer: AIOKafkaProducer | None = None


def init_db() -> None:
    conn = psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        dbname=os.getenv("POSTGRES_DB", "paymentdb"),
        user=os.getenv("POSTGRES_USER", "payment"),
        password=os.getenv("POSTGRES_PASSWORD", "paymentpass"),
        port=5432,
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS payment_ledger (
                id SERIAL PRIMARY KEY,
                payment_id TEXT NOT NULL,
                amount NUMERIC(12,2) NOT NULL,
                currency TEXT NOT NULL,
                customer_id TEXT NOT NULL,
                merchant_id TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
    conn.commit()
    conn.close()


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
        except Exception as exc:
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
                        f"[ledger] Routed payment {payload.get('payment_id')} to {DLQ_TOPIC} "
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
    conn = psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        dbname=os.getenv("POSTGRES_DB", "paymentdb"),
        user=os.getenv("POSTGRES_USER", "payment"),
        password=os.getenv("POSTGRES_PASSWORD", "paymentpass"),
        port=5432,
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO payment_ledger (payment_id, amount, currency, customer_id, merchant_id, status)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                payload.get("payment_id"),
                payload.get("amount"),
                payload.get("currency"),
                payload.get("customer_id"),
                payload.get("merchant_id"),
                payload.get("status", "received"),
            ),
        )
    conn.commit()
    conn.close()
    payload["status"] = "settled"
    await producer.send_and_wait(
        SETTLED_TOPIC,
        key=payload["payment_id"].encode("utf-8"),
        value=json.dumps(payload).encode("utf-8"),
    )
    LEDGER_WRITES.inc()
    print(f"[ledger] Recorded payment {payload.get('payment_id')}")


async def consume() -> None:
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="ledger-group",
    )
    await consumer.start()
    try:
        async for msg in consumer:
            payload = json.loads(msg.value.decode("utf-8"))
            try:
                await process_with_retry(
                    lambda payload=payload: process_payment(payload),
                    producer=producer,
                    original_topic=TOPIC,
                    payload=payload,
                )
            except Exception as exc:
                print(f"[ledger] Failed to process payment {payload.get('payment_id')}: {exc}")
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
