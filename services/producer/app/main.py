from __future__ import annotations

import json
import os
import random
import asyncio
import uuid
from typing import Any
from pathlib import Path

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException
from jsonschema import ValidationError, validate
from pydantic import BaseModel, Field
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

TOPIC = "payments.raw"
GENERATOR_INTERVAL_SECONDS = float(os.getenv("PAYMENT_GENERATOR_INTERVAL_SECONDS", "5"))
SUSPICIOUS_RATE = float(os.getenv("PAYMENT_GENERATOR_SUSPICIOUS_RATE", "0.15"))
GENERATION_RUN_ID = uuid.uuid4().hex[:8]

app = FastAPI(title="Payment Producer API")

PAYMENT_REQUESTS = Counter("payment_requests_total", "Total payment requests received")
PAYMENT_SUCCESS = Counter("payment_success_total", "Successful payment events produced")
PAYMENT_ERRORS = Counter("payment_errors_total", "Failed payment events")
PAYMENT_LATENCY = Histogram("payment_processing_latency_seconds", "Latency for payment processing")
PAYMENT_PENDING = Gauge("payment_pending", "Payments currently being published")

producer: AIOKafkaProducer | None = None
generator_task: asyncio.Task[None] | None = None
SCHEMA_PATH = Path(os.getenv("PAYMENT_EVENT_SCHEMA_PATH", "/app/schemas/payment_event.schema.json"))
with SCHEMA_PATH.open(encoding="utf-8") as schema_file:
    PAYMENT_EVENT_SCHEMA = json.load(schema_file)


class PaymentRequest(BaseModel):
    payment_id: str = Field(..., min_length=1)
    amount: float = Field(..., gt=0)
    currency: str = Field(..., min_length=3, max_length=3)
    customer_id: str = Field(..., min_length=1)
    merchant_id: str = Field(..., min_length=1)


async def publish_payment(payload: PaymentRequest) -> None:
    if producer is None:
        raise RuntimeError("Kafka producer unavailable")

    event = {
        "payment_id": payload.payment_id,
        "customer_id": payload.customer_id,
        "amount": payload.amount,
        "currency": payload.currency,
        "merchant_id": payload.merchant_id,
        "status": "received",
    }
    validate(instance=event, schema=PAYMENT_EVENT_SCHEMA)
    await producer.send_and_wait(
        TOPIC,
        key=payload.payment_id.encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
    )


def random_payment(sequence_number: int = 0) -> PaymentRequest:
    scenario_step = sequence_number % 12
    scenario_id = sequence_number // 12
    if scenario_step == 0:
        customer_id = f"demo-behavior-{GENERATION_RUN_ID}-{scenario_id}"
        amount = 100.0
    elif scenario_step == 1:
        customer_id = f"demo-behavior-{GENERATION_RUN_ID}-{scenario_id}"
        amount = 300.0
    elif scenario_step in (2, 3, 4):
        customer_id = f"demo-velocity-{GENERATION_RUN_ID}-{scenario_id}"
        amount = 50.0 + scenario_step
    elif scenario_step == 5:
        customer_id = f"demo-new-user-{GENERATION_RUN_ID}-{scenario_id}"
        amount = 6000.0
    else:
        suspicious = random.random() < SUSPICIOUS_RATE
        customer_id = f"cust-{random.randint(1000, 9999)}"
        amount = random.uniform(1001, 5000) if suspicious else random.uniform(5, 750)

    payment_id = f"pay-{uuid.uuid4()}"
    return PaymentRequest(
        payment_id=payment_id,
        amount=round(amount, 2),
        currency=random.choice(("USD", "EUR", "GBP")),
        customer_id=customer_id,
        merchant_id=f"merchant-{random.randint(100, 999)}",
    )


async def generate_payments() -> None:
    sequence_number = 0
    while True:
        payment = random_payment(sequence_number)
        PAYMENT_REQUESTS.inc()
        PAYMENT_PENDING.inc()
        try:
            with PAYMENT_LATENCY.time():
                await publish_payment(payment)
            PAYMENT_SUCCESS.inc()
            print(f"[producer] Published {payment.payment_id} (${payment.amount:.2f})")
        except Exception as exc:  # pragma: no cover
            PAYMENT_ERRORS.inc()
            print(f"[producer] Failed to publish generated payment: {exc}")
        finally:
            PAYMENT_PENDING.dec()
        sequence_number += 1
        await asyncio.sleep(GENERATOR_INTERVAL_SECONDS)


@app.on_event("startup")
async def startup_event() -> None:
    global producer, generator_task
    producer = AIOKafkaProducer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    await producer.start()
    generator_task = asyncio.create_task(generate_payments())


@app.on_event("shutdown")
async def shutdown_event() -> None:
    global producer, generator_task
    if generator_task is not None:
        generator_task.cancel()
        await asyncio.gather(generator_task, return_exceptions=True)
    if producer is not None:
        await producer.stop()


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/payments")
async def create_payment(payload: PaymentRequest) -> dict[str, Any]:
    PAYMENT_REQUESTS.inc()
    PAYMENT_PENDING.inc()
    with PAYMENT_LATENCY.time():
        try:
            if producer is None:
                PAYMENT_ERRORS.inc()
                raise HTTPException(status_code=503, detail="Kafka producer unavailable")
            await publish_payment(payload)
            PAYMENT_SUCCESS.inc()
            return {"status": "accepted", "payment_id": payload.payment_id, "topic": TOPIC}
        except ValidationError as exc:
            PAYMENT_ERRORS.inc()
            raise HTTPException(status_code=500, detail=f"Invalid payment event: {exc.message}") from exc
        except HTTPException:
            raise
        except Exception as exc:  # pragma: no cover
            PAYMENT_ERRORS.inc()
            raise HTTPException(status_code=500, detail=f"Failed to publish payment: {exc}") from exc
        finally:
            PAYMENT_PENDING.dec()
