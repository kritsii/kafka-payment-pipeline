import asyncio
import json
import os

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI
from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

INPUT_TOPIC = "payments.raw"
VALIDATED_TOPIC = "payments.validated"
FRAUD_TOPIC = "payments.fraud"

app = FastAPI(title="Fraud Service")
FRAUD_CHECKS = Counter("fraud_checks_total", "Fraud checks performed")
FRAUD_FLAGS = Counter("fraud_flags_total", "Fraud flagged payments")
producer: AIOKafkaProducer | None = None


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
            FRAUD_CHECKS.inc()
            amount = float(payload.get("amount", 0))
            transaction_key = payload["payment_id"].encode("utf-8")
            if amount > 1000:
                FRAUD_FLAGS.inc()
                payload["status"] = "fraud"
                await producer.send_and_wait(
                    FRAUD_TOPIC,
                    key=transaction_key,
                    value=json.dumps(payload).encode("utf-8"),
                )
                print(f"[fraud] Flagged payment {payload.get('payment_id')} as suspicious")
            else:
                payload["status"] = "validated"
                await producer.send_and_wait(
                    VALIDATED_TOPIC,
                    key=transaction_key,
                    value=json.dumps(payload).encode("utf-8"),
                )
                print(f"[fraud] Approved payment {payload.get('payment_id')}")
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
