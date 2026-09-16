import asyncio
import json
import os
import uuid
from collections import defaultdict

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

DLQ_TOPIC = "payments.dlq"

app = FastAPI(title="Payment Admin Service")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)
DLQ_REPLAYED = Counter("dlq_replayed_total", "Number of DLQ messages replayed")
DLQ_INSPECTED = Counter("dlq_inspected_total", "Number of DLQ messages inspected")

producer: AIOKafkaProducer | None = None
consumer: AIOKafkaConsumer | None = None
seen_dlq_messages: list[dict] = []
messages_by_payment_id: defaultdict[str, list[dict]] = defaultdict(list)


async def consume_dlq() -> None:
    global consumer
    consumer = AIOKafkaConsumer(
        DLQ_TOPIC,
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=f"admin-dlq-{uuid.uuid4().hex}",
    )
    await consumer.start()
    try:
        async for msg in consumer:
            try:
                payload = json.loads(msg.value.decode("utf-8"))
                seen_dlq_messages.append(payload)
                messages_by_payment_id[str(payload.get("payment_id", "unknown"))].append(payload)
                DLQ_INSPECTED.inc()
                print(f"[admin] DLQ message observed for payment {payload.get('payment_id')}")
                await consumer.commit()
            except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
                await consumer.commit()
                print(f"[admin] Dropped malformed DLQ event: {exc}")
    finally:
        await consumer.stop()


@app.on_event("startup")
async def startup_event() -> None:
    global producer
    producer = AIOKafkaProducer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    await producer.start()
    asyncio.create_task(consume_dlq())


@app.on_event("shutdown")
async def shutdown_event() -> None:
    if consumer is not None:
        await consumer.stop()
    if producer is not None:
        await producer.stop()


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/dlq")
async def list_dlq() -> dict[str, list[dict]]:
    return {"items": seen_dlq_messages}


@app.get("/dlq/{payment_id}")
async def get_dlq_by_payment(payment_id: str) -> dict[str, list[dict]]:
    return {"items": messages_by_payment_id.get(payment_id, [])}


@app.post("/dlq/replay/{payment_id}")
async def replay_dlq(payment_id: str) -> dict[str, object]:
    if producer is None:
        raise HTTPException(status_code=503, detail="Kafka producer unavailable")

    items = messages_by_payment_id.get(payment_id, [])
    if not items:
        raise HTTPException(status_code=404, detail=f"No DLQ event found for payment {payment_id}")

    latest = items[-1]
    source_topic = latest.get("source_topic") or "payments.raw"
    original_payload = latest.get("original_payload", {})

    await producer.send_and_wait(
        source_topic,
        key=str(payment_id).encode("utf-8"),
        value=json.dumps(original_payload).encode("utf-8"),
    )
    DLQ_REPLAYED.inc()
    return {
        "status": "replayed",
        "payment_id": payment_id,
        "source_topic": source_topic,
        "payload": original_payload,
    }
