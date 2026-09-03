# Payment Processing Pipeline

This project creates an end-to-end payment processing pipeline with a React/MUI
dashboard and FastAPI dashboard API:

- Kafka (KRaft mode)
- Producer (FastAPI)
- Notification consumer
- Ledger consumer
- Fraud consumer
- PostgreSQL
- Prometheus
- Dashboard API (FastAPI)
- Dashboard (React/MUI)

## Quick Start

```bash
docker compose up --build -d
```

## Services

- Producer API: http://localhost:8000
- FastAPI Swagger: http://localhost:8000/docs
- Admin DLQ API: http://localhost:8004/docs
- Dashboard API: http://localhost:8005/docs
- Dashboard UI: run `npm install && npm run dev` inside `frontend/`, then open http://localhost:5173
- Prometheus: http://localhost:9090
- PostgreSQL: localhost:5432

## Kafka topics

The stack provisions this topic when it starts:

| Topic | Partitions | Consumers |
| --- | ---: | --- |
| `payments.raw` | 3 | fraud |
| `payments.validated` | 3 | ledger |
| `payments.settled` | 3 | notification |
| `payments.fraud` | 3 | notification |
| `payments.dlq` | 3 | admin |

Events use `payment_id` as the Kafka key, keeping each transaction ordered. The
`customer_id` identifies the customer for fraud and spending analysis. All services
share and use the contract in `schemas/payment_event.schema.json`, mounted read-only
at `/app/schemas/payment_event.schema.json`. Event statuses are `received`,
`validated`, `fraud`, `settled`, and `dlq` for failed messages. payThe fraud and
ledger consumers use exponential backoff retries and route exhausted failures to
`payments.dlq` for inspection or replay.

The producer also publishes a generated payment every 5 seconds. Set
`PAYMENT_GENERATOR_INTERVAL_SECONDS` or `PAYMENT_GENERATOR_SUSPICIOUS_RATE` in
the producer environment to change the cadence or suspicious-event rate.

To list the topics from the Kafka container:

```bash
docker compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --list
```

## Example payment request

```bash
curl -X POST http://localhost:8000/payments \
  -H "Content-Type: application/json" \
  -d '{
    "payment_id": "p-1001",
    "amount": 125.50,
    "currency": "USD",
    "customer_id": "cust-42",
    "merchant_id": "merchant-7"
  }'
```

## DLQ and replay flow

The pipeline now includes a `payments.dlq` topic. When fraud or ledger processing fails after the configured retry window, the service publishes a diagnostic record with the original payload, attempt count, source topic, and exception metadata. The admin service consumes that topic and exposes endpoints to inspect the queue or replay a payment back to its original topic for investigation.

Example:

```bash
curl http://localhost:8004/dlq
curl http://localhost:8004/dlq/<payment_id>
curl -X POST http://localhost:8004/dlq/replay/<payment_id>
```

## Monitoring

Prometheus scrapes metrics from the producer and stream consumers.

## Shutdown

```bash
docker compose down -v
```
