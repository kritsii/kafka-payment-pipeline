# Payment Processing Pipeline

This project creates an end-to-end payment processing pipeline using 8 containers:

- Kafka (KRaft mode)
- Producer (FastAPI)
- Notification consumer
- Ledger consumer
- Fraud consumer
- PostgreSQL
- Prometheus
- Grafana

## Quick Start

```bash
docker compose up --build -d
```

## Services

- Producer API: http://localhost:8000
- FastAPI Swagger: http://localhost:8000/docs
- Grafana: http://localhost:3000 (admin/admin)
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

Events use `payment_id` as the Kafka key, keeping each transaction ordered. The
`customer_id` identifies the customer for fraud and spending analysis. All services
share and use the contract in `schemas/payment_event.schema.json`, mounted read-only
at `/app/schemas/payment_event.schema.json`. Event statuses are `received`,
`validated`, `fraud`, and `settled`.

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

## Monitoring

Prometheus scrapes metrics from the producer and stream consumers. Grafana is provisioned with a Prometheus datasource.

## Shutdown

```bash
docker compose down -v
```
