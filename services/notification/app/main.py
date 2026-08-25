import asyncio
import json
import os
from typing import Any

from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI
from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

TOPICS = ("payments.settled", "payments.fraud")

app = FastAPI(title="Notification Service")
NOTIFY_COUNT = Counter("notification_events_total", "Notification events processed")

consumer: AIOKafkaConsumer | None = None


async def consume() -> None:
    global consumer
    consumer = AIOKafkaConsumer(
        *TOPICS,
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="notification-group",
    )
    await consumer.start()
    try:
        async for msg in consumer:
            payload = json.loads(msg.value.decode("utf-8"))
            NOTIFY_COUNT.inc()
            print(
                f"[notification] Sent SMS/email for {msg.topic} payment "
                f"{payload.get('payment_id')}"
            )
    finally:
        await consumer.stop()


@app.on_event("startup")
async def startup_event() -> None:
    asyncio.create_task(consume())


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
