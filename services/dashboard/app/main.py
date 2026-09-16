import asyncio
import os
import time
from datetime import datetime, timezone

import httpx
import psycopg
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://localhost:9090")
DASHBOARD_STARTED_AT = datetime.now(timezone.utc).replace(tzinfo=None)


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
                          status, risk_score, risk_reasons, created_at, id
                      FROM (
                       SELECT DISTINCT ON (payment_id)
                           payment_id, amount, currency, customer_id, merchant_id,
                           status, risk_score, risk_reasons, created_at, id
                              FROM payment_ledger
                              WHERE created_at >= %s
                       ORDER BY payment_id, created_at DESC, id DESC
                      ) latest
                      ORDER BY created_at DESC, id DESC
                    LIMIT %s
                    """,
                        (DASHBOARD_STARTED_AT, limit),
                )
                rows = cursor.fetchall()
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        transactions = []
        for payment_id, amount, currency, customer_id, merchant_id, status, risk_score, risk_reasons, created_at, _ in rows:
            age_minutes = max(0, int((now - created_at).total_seconds() // 60))
            display_status = {
                "settled": "Settled",
                "rejected": "Fraud rejected",
            }.get(status, "Processing")
            stage = 5 if status == "settled" else 3 if status == "rejected" else 4
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
                    "ledger": "Posted" if status == "settled" else "Blocked" if status == "rejected" else "Queued",
                    "notification": "Sent" if status == "settled" else "Not sent" if status == "rejected" else "Queued",
                    "riskScore": risk_score,
                    "riskReasons": risk_reasons or [],
                    "time": f"{age_minutes} min ago",
                    "ageMinutes": age_minutes,
                }
            )
        return transactions
    except (psycopg.Error, TypeError, ValueError):
        return []


def query_transaction_summary() -> dict[str, int]:
    summary = {"total": 0, "successful": 0, "failed": 0, "review": 0}
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
                    SELECT status, COUNT(*)
                    FROM (
                        SELECT DISTINCT ON (payment_id) payment_id, status
                        FROM payment_ledger
                        WHERE created_at >= %s
                        ORDER BY payment_id, created_at DESC, id DESC
                    ) latest
                    GROUP BY status
                    """,
                    (DASHBOARD_STARTED_AT,),
                )
                for status, count in cursor.fetchall():
                    summary["total"] += count
                    if status == "settled":
                        summary["successful"] += count
                    elif status == "rejected":
                        summary["failed"] += count
                    elif status == "review":
                        summary["review"] += count
    except (psycopg.Error, TypeError, ValueError):
        pass
    return summary

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


async def query_settlement_chart() -> list[dict[str, object]]:
    try:
        async with httpx.AsyncClient(timeout=2.5) as client:
            range_params = {
                "start": max(time.time() - 7 * 60 * 60, DASHBOARD_STARTED_AT.timestamp()),
                "end": time.time(),
                "step": 300,
            }
            settlement_response, fraud_response = await asyncio.gather(
                client.get(
                    f"{PROMETHEUS_URL}/api/v1/query_range",
                    params={**range_params, "query": "sum(rate(ledger_writes_total[5m])) * 3600"},
                ),
                client.get(
                    f"{PROMETHEUS_URL}/api/v1/query_range",
                    params={**range_params, "query": "sum(rate(fraud_flags_total[5m])) / sum(rate(fraud_checks_total[5m])) * 100"},
                ),
            )
            settlement_response.raise_for_status()
            fraud_response.raise_for_status()
            settlement_results = settlement_response.json().get("data", {}).get("result", [])
            fraud_results = fraud_response.json().get("data", {}).get("result", [])
            settlement_values = settlement_results[0].get("values", []) if settlement_results else []
            fraud_values = fraud_results[0].get("values", []) if fraud_results else []
            fraud_by_timestamp = {str(timestamp): value for timestamp, value in fraud_values}
            return [
                {
                    "time": datetime.fromtimestamp(float(timestamp), timezone.utc).strftime("%H:%M"),
                    "settlementVolume": round(float(value), 1),
                    "fraudRate": round(float(fraud_by_timestamp.get(str(timestamp), 0)), 2),
                }
                for timestamp, value in settlement_values
            ]
    except (httpx.HTTPError, ValueError, KeyError, IndexError):
        return []


async def overview_metrics() -> dict[str, object]:
    latency, settlement_chart = await asyncio.gather(
        query_prometheus(
            "sum(rate(payment_processing_latency_seconds_sum[5m])) / "
            "sum(rate(payment_processing_latency_seconds_count[5m]))"
        ),
        query_settlement_chart(),
    )
    services = await query_series("up{job=~\"producer|notification|ledger|fraud\"}")
    service_health = [
        {
            "name": item.get("metric", {}).get("job", "unknown"),
            "status": "healthy" if float(item.get("value", [0, 0])[1]) == 1 else "degraded",
        }
        for item in services
    ]

    transaction_summary = query_transaction_summary()
    total = transaction_summary["total"]
    successful = transaction_summary["successful"]
    failed = transaction_summary["failed"]
    review = transaction_summary["review"]
    return {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "transactions": {
            "total": total,
            "successful": successful,
            "failed": failed,
            "review": review,
            "items": query_transactions(limit=100),
        },
        "rates": {
            "success": (successful / total * 100) if total else 0,
            "failure": (failed / total * 100) if total else 0,
        },
        "settlement": {"points": settlement_chart},
        "latency_seconds": latency or 0,
        "services": service_health,
    }


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/overview")
async def get_overview() -> dict[str, object]:
    return await overview_metrics()