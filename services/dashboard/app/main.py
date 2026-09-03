import asyncio
import os
import time
from datetime import datetime, timezone

import httpx
import psycopg
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://localhost:9090")


def query_transactions(limit: int = 100) -> list[dict[str, object]]:
    try:
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
                    SELECT payment_id, amount, currency, customer_id, merchant_id,
                           status, created_at
                    FROM payment_ledger
                    ORDER BY created_at DESC, id DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = cursor.fetchall()
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        transactions = []
        for payment_id, amount, currency, customer_id, merchant_id, status, created_at in rows:
            age_minutes = max(0, int((now - created_at).total_seconds() // 60))
            display_status = "Settled" if status == "settled" else "Processing"
            stage = 5 if status == "settled" else 4
            transactions.append(
                {
                    "id": payment_id,
                    "user": customer_id,
                    "merchant": merchant_id,
                    "amount": float(amount),
                    "currency": currency,
                    "method": "Unknown",
                    "status": display_status,
                    "stage": stage,
                    "retries": 0,
                    "ledger": "Posted" if status == "settled" else "Pending",
                    "notification": "Sent" if status == "settled" else "Queued",
                    "time": f"{age_minutes} min ago",
                    "ageMinutes": age_minutes,
                }
            )
        return transactions
    except (psycopg.Error, TypeError, ValueError):
        return []

app = FastAPI(title="Payment Dashboard API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


async def query_prometheus(query: str) -> float | None:
    try:
        async with httpx.AsyncClient(timeout=2.5) as client:
            response = await client.get(
                f"{PROMETHEUS_URL}/api/v1/query", params={"query": query}
            )
            response.raise_for_status()
            results = response.json().get("data", {}).get("result", [])
            if not results:
                return None
            return float(results[0].get("value", [0, 0])[1])
    except (httpx.HTTPError, ValueError, KeyError, IndexError):
        return None


async def query_series(query: str) -> list[dict[str, object]]:
    try:
        async with httpx.AsyncClient(timeout=2.5) as client:
            response = await client.get(
                f"{PROMETHEUS_URL}/api/v1/query", params={"query": query}
            )
            response.raise_for_status()
            return response.json().get("data", {}).get("result", [])
    except (httpx.HTTPError, ValueError, KeyError):
        return []


async def query_volume() -> list[dict[str, object]]:
    try:
        async with httpx.AsyncClient(timeout=2.5) as client:
            response = await client.get(
                f"{PROMETHEUS_URL}/api/v1/query_range",
                params={
                    "query": "sum(rate(payment_requests_total[5m])) * 3600",
                    "start": time.time() - 7 * 60 * 60,
                    "end": time.time(),
                    "step": 3600,
                },
            )
            response.raise_for_status()
            results = response.json().get("data", {}).get("result", [])
            values = results[0].get("values", []) if results else []
            return [
                {
                    "time": datetime.fromtimestamp(float(timestamp), timezone.utc).strftime("%H:%M"),
                    "volume": round(float(value), 1),
                }
                for timestamp, value in values
            ]
    except (httpx.HTTPError, ValueError, KeyError, IndexError):
        return []


async def overview_metrics() -> dict[str, object]:
    requests, successes, errors, pending, latency, queue_lag, volume = await asyncio.gather(
        query_prometheus("sum(payment_requests_total)"),
        query_prometheus("sum(payment_success_total)"),
        query_prometheus("sum(payment_errors_total)"),
        query_prometheus("sum(payment_pending)"),
        query_prometheus(
            "sum(rate(payment_processing_latency_seconds_sum[5m])) / "
            "sum(rate(payment_processing_latency_seconds_count[5m]))"
        ),
        query_prometheus("sum(kafka_consumergroup_lag)"),
        query_volume(),
    )
    services = await query_series("up{job=~\"producer|notification|ledger|fraud\"}")
    service_health = [
        {
            "name": item.get("metric", {}).get("job", "unknown"),
            "status": "healthy" if float(item.get("value", [0, 0])[1]) == 1 else "degraded",
        }
        for item in services
    ]

    total = requests or 0
    successful = successes or 0
    failed = errors or 0
    return {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "transactions": {
            "total": total,
            "successful": successful,
            "failed": failed,
            "pending": pending or 0,
            "items": query_transactions(),
        },
        "rates": {
            "success": (successful / total * 100) if total else 0,
            "failure": (failed / total * 100) if total else 0,
        },
        "volume": {"points": volume},
        "latency_seconds": latency or 0,
        "services": service_health,
        "queue_lag": {
            "value": queue_lag,
            "status": "available" if queue_lag is not None else "not_instrumented",
        },
    }


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/overview")
async def get_overview() -> dict[str, object]:
    return await overview_metrics()